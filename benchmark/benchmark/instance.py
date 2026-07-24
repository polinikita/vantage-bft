# Copyright(C) Facebook, Inc. and its affiliates.
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict, OrderedDict
from time import sleep

from benchmark.utils import Print, BenchError, progress_bar
from benchmark.settings import Settings, SettingsError


class AWSError(Exception):
    def __init__(self, error):
        assert isinstance(error, ClientError)
        self.message = error.response['Error']['Message']
        self.code = error.response['Error']['Code']
        super().__init__(self.message)


class InstanceManager:
    INSTANCE_NAME = 'dag-node'
    SECURITY_GROUP_NAME = 'dag'

    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.clients = OrderedDict()
        for region in settings.aws_regions:
            self.clients[region] = boto3.client('ec2', region_name=region)

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

    def _get(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        #
        # Collects PUBLIC and PRIVATE IPs side by side (single
        # describe_instances call per region) so the two stay index-aligned
        # per instance. `None` is recorded (not dropped) when an instance
        # transiently lacks one of the two (e.g. still 'pending' and not yet
        # assigned an address) -- dropping it here would desync `ids`/
        # `public_ips`/`private_ips` against each other; callers that need a
        # complete pair (the committee vs. SSH-connection host lists) filter
        # `None`s out themselves, see `_paired_hosts`.
        ids = defaultdict(list)
        public_ips, private_ips = defaultdict(list), defaultdict(list)
        for region, client in self.clients.items():
            r = client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [self.INSTANCE_NAME]
                    },
                    {
                        'Name': 'instance-state-name',
                        'Values': state
                    }
                ]
            )
            instances = [y for x in r['Reservations'] for y in x['Instances']]
            for x in instances:
                ids[region] += [x['InstanceId']]
                public_ips[region] += [x.get('PublicIpAddress')]
                private_ips[region] += [x.get('PrivateIpAddress')]
        return ids, public_ips, private_ips

    def _paired_hosts(self, state):
        ''' Per-region list of (public_ip, private_ip) pairs for instances in
        `state`, built from a single `_get()` snapshot so the two addresses
        stay tied to the same physical instance. Any instance missing either
        address is dropped (with a warning) rather than left to silently
        shift the pairing of every instance after it -- see `_get`. '''
        _, public_ips, private_ips = self._get(state)
        pairs = defaultdict(list)
        for region in public_ips:
            for pub, priv in zip(public_ips[region], private_ips[region]):
                if pub is None or priv is None:
                    Print.warn(
                        f'Instance in {region} is missing its public or '
                        f'private IP (skipped; likely still booting)'
                    )
                    continue
                pairs[region].append((pub, priv))
        return pairs

    def _wait(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        while True:
            sleep(1)
            ids, _, _ = self._get(state)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_security_group(self, client):
        client.create_security_group(
            Description='HotStuff node',
            GroupName=self.SECURITY_GROUP_NAME,
        )

        client.authorize_security_group_ingress(
            GroupName=self.SECURITY_GROUP_NAME,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 22,
                    'ToPort': 22,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Debug SSH access',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Debug SSH access',
                    }],
                },
                {
                    'IpProtocol': 'tcp',
                    'FromPort': self.settings.base_port,
                    'ToPort': self.settings.base_port + 2_000,
                    'IpRanges': [{
                        'CidrIp': '0.0.0.0/0',
                        'Description': 'Dag port',
                    }],
                    'Ipv6Ranges': [{
                        'CidrIpv6': '::/0',
                        'Description': 'Dag port',
                    }],
                }
            ]
        )

    # Canonical's official AWS account id (stable across regions/time).
    CANONICAL_OWNER_ID = '099720109477'

    def _get_ami(self, client):
        # The AMI id changes per region and the old fixed build-date
        # description (2020-10-26 focal) is long deregistered, so resolve
        # the newest available Ubuntu 22.04 LTS amd64 HVM/EBS image by
        # owner + name glob instead of pinning an ImageId or exact date.
        response = client.describe_images(
            Owners=[self.CANONICAL_OWNER_ID],
            Filters=[
                {
                    'Name': 'name',
                    'Values': [
                        'ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*'
                    ]
                },
                {'Name': 'state', 'Values': ['available']},
                {'Name': 'root-device-type', 'Values': ['ebs']},
                {'Name': 'virtualization-type', 'Values': ['hvm']},
            ]
        )
        images = sorted(
            response['Images'], key=lambda x: x['CreationDate'], reverse=True
        )
        if not images:
            raise BenchError(
                'AMI resolution',
                Exception('No matching Ubuntu 22.04 AMI found in region')
            )
        return images[0]['ImageId']

    def create_instances(self, instances):
        assert isinstance(instances, int) and instances > 0

        # Create the security group in every region.
        for client in self.clients.values():
            try:
                self._create_security_group(client)
            except ClientError as e:
                error = AWSError(e)
                if error.code != 'InvalidGroup.Duplicate':
                    raise BenchError('Failed to create security group', error)

        try:
            # Create all instances.
            size = instances * len(self.clients)
            # Opt-in EC2 Spot (settings.json instances.spot == true). One-time
            # requests, terminate-on-interruption, and NO MaxPrice -- boto3 then
            # caps the bid at the on-demand price, so Spot never costs more than
            # on-demand while still yielding the usual discount, and there is no
            # surprise-price exposure. Absent/false -> {} -> on-demand, i.e. the
            # exact prior run_instances call (byte-identical request).
            spot_options = {}
            if getattr(self.settings, 'spot', False):
                spot_options = {
                    'InstanceMarketOptions': {
                        'MarketType': 'spot',
                        'SpotOptions': {
                            'SpotInstanceType': 'one-time',
                            'InstanceInterruptionBehavior': 'terminate',
                        },
                    }
                }
            progress = progress_bar(
                self.clients.values(), prefix=f'Creating {size} instances'
            )
            for client in progress:
                client.run_instances(
                    ImageId=self._get_ami(client),
                    InstanceType=self.settings.instance_type,
                    KeyName=self.settings.key_name,
                    MaxCount=instances,
                    MinCount=instances,
                    SecurityGroups=[self.SECURITY_GROUP_NAME],
                    TagSpecifications=[{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'Name',
                            'Value': self.INSTANCE_NAME
                        }]
                    }],
                    EbsOptimized=True,
                    BlockDeviceMappings=[{
                        'DeviceName': '/dev/sda1',
                        'Ebs': {
                            'VolumeType': 'gp2',
                            'VolumeSize': 200,
                            'DeleteOnTermination': True
                        }
                    }],
                    **spot_options,
                )

            # Wait for the instances to boot.
            Print.info('Waiting for all instances to boot...')
            self._wait(['pending'])
            Print.heading(f'Successfully created {size} new instances')
        except ClientError as e:
            raise BenchError('Failed to create AWS instances', AWSError(e))

    def terminate_instances(self):
        try:
            ids, _, _ = self._get(['pending', 'running', 'stopping', 'stopped'])
            size = sum(len(x) for x in ids.values())
            if size == 0:
                Print.heading(f'All instances are shut down')
                return

            # Terminate instances.
            for region, client in self.clients.items():
                if ids[region]:
                    client.terminate_instances(InstanceIds=ids[region])

            # Wait for all instances to properly shut down.
            Print.info('Waiting for all instances to shut down...')
            self._wait(['shutting-down'])
            for client in self.clients.values():
                client.delete_security_group(
                    GroupName=self.SECURITY_GROUP_NAME
                )

            Print.heading(f'Testbed of {size} instances destroyed')
        except ClientError as e:
            raise BenchError('Failed to terminate instances', AWSError(e))

    def start_instances(self, max):
        size = 0
        try:
            ids, _, _ = self._get(['stopping', 'stopped'])
            for region, client in self.clients.items():
                if ids[region]:
                    target = ids[region]
                    target = target if len(target) < max else target[:max]
                    size += len(target)
                    client.start_instances(InstanceIds=target)
            Print.heading(f'Starting {size} instances')
        except ClientError as e:
            raise BenchError('Failed to start instances', AWSError(e))

    def stop_instances(self):
        try:
            ids, _, _ = self._get(['pending', 'running'])
            for region, client in self.clients.items():
                if ids[region]:
                    client.stop_instances(InstanceIds=ids[region])
            size = sum(len(x) for x in ids.values())
            Print.heading(f'Stopping {size} instances')
        except ClientError as e:
            raise BenchError(AWSError(e))

    def hosts(self, flat=False):
        ''' PUBLIC (internet-routable) IPs -- the SSH/rsync/tmux connection
        targets used from the coordinator laptop. NOT for the Committee
        (node<->node/client<->node): see `internal_hosts()`. '''
        try:
            pairs = self._paired_hosts(['pending', 'running'])
            ips = {region: [pub for pub, _ in v] for region, v in pairs.items()}
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances IPs', AWSError(e))

    def internal_hosts(self, flat=False):
        ''' PRIVATE (VPC-internal) IPs, index-aligned per region with
        `hosts()` (both are read from a single `_paired_hosts()` snapshot per
        call). The Committee (primary/worker/consensus/transactions/metrics
        addresses -- everything nodes and collocated clients talk to each
        other over) must be built from these, never from `hosts()`'s public
        IPs: same-region node<->node traffic over public IPs is billed
        cross-instance data transfer and routes through the internet edge
        instead of the VPC, which is what collapsed a live 20-node run to
        ~1.6k tx/s at 50k offered.

        Residual risk: `hosts()` and `internal_hosts()` are two independent
        `describe_instances` calls, so pairing across the two calls relies on
        AWS returning the same per-region instance order both times (true in
        practice for an unchanged, non-transitioning fleet queried
        back-to-back, which is how `_select_hosts`/`_select_hosts_config`
        call them in `run()`) rather than on a shared API response. '''
        try:
            pairs = self._paired_hosts(['pending', 'running'])
            ips = {region: [priv for _, priv in v] for region, v in pairs.items()}
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances private IPs', AWSError(e))

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for region, ips in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, ip in enumerate(ips):
                new_line = '\n' if (i+1) % 6 == 0 else ''
                text += f'{new_line} {i}\tssh -i {key} ubuntu@{ip}\n'
        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )
