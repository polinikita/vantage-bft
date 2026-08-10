# Copyright(C) Facebook, Inc. and its affiliates.
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
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
    # Keep the metrics collector separate from validators.
    COLLECTOR_NAME = 'metrics-collector'
    SECURITY_GROUP_NAME = 'dag'
    # Prometheus API port.
    MONITOR_PORT = 9090
    # Grafana UI port.
    GRAFANA_PORT = 3000

    # Fallback prices when Spot pricing is unavailable.
    ON_DEMAND_FALLBACK_PRICE = {
        'c5.xlarge': 0.192,  # eu-west-1
        'c5d.xlarge': 0.216,  # eu-west-1
    }
    DEFAULT_FALLBACK_PRICE = 0.192

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

    def _get(self, state, name=None):
        # With no name, include validators and the metrics collector.
        # Keep public and private IP lists aligned.
        ids = defaultdict(list)
        public_ips, private_ips = defaultdict(list), defaultdict(list)
        tag_values = [name] if name is not None else [self.INSTANCE_NAME, self.COLLECTOR_NAME]
        for region, client in self.clients.items():
            r = client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': tag_values
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

    def _paired_hosts(self, state, name=None):
        '''Return per-region public/private pairs; skip incomplete instances.'''
        _, public_ips, private_ips = self._get(state, name)
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

    def _wait(self, state, name=None):
        while True:
            sleep(1)
            ids, _, _ = self._get(state, name)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_security_group(self, client):
        '''Create the security group and ensure each ingress rule exists.'''
        try:
            client.create_security_group(
                Description='HotStuff node',
                GroupName=self.SECURITY_GROUP_NAME,
            )
        except ClientError as e:
            if AWSError(e).code != 'InvalidGroup.Duplicate':
                raise

        permissions = [
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
            },
            {
                # Prometheus API access.
                'IpProtocol': 'tcp',
                'FromPort': self.MONITOR_PORT,
                'ToPort': self.MONITOR_PORT,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0',
                    'Description': 'Metrics-collector Prometheus HTTP API',
                }],
                'Ipv6Ranges': [{
                    'CidrIpv6': '::/0',
                    'Description': 'Metrics-collector Prometheus HTTP API',
                }],
            },
            {
                # Grafana UI access.
                'IpProtocol': 'tcp',
                'FromPort': self.GRAFANA_PORT,
                'ToPort': self.GRAFANA_PORT,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0',
                    'Description': 'Metrics-collector Grafana UI',
                }],
                'Ipv6Ranges': [{
                    'CidrIpv6': '::/0',
                    'Description': 'Metrics-collector Grafana UI',
                }],
            },
        ]
        for permission in permissions:
            try:
                client.authorize_security_group_ingress(
                    GroupName=self.SECURITY_GROUP_NAME,
                    IpPermissions=[permission],
                )
            except ClientError as e:
                if AWSError(e).code != 'InvalidPermission.Duplicate':
                    raise

    def _resolve_az_subnet(self, client, region):
        '''Resolve an AZ, default subnet, and VPC; keep instances in one zone.'''
        configured_az = getattr(self.settings, 'availability_zone', None)
        az = None
        mapping_form = isinstance(configured_az, dict)
        if mapping_form:
            # Omitted regions use automatic selection.
            az = configured_az.get(region)
        elif configured_az:
            az = configured_az

        # The availability zone must belong to the selected region.
        if az is not None and not az.startswith(region):
            hint = (
                f"entry for '{region}' in the instances.availability_zone "
                f"mapping names an AZ in another region"
                if mapping_form else
                "For a multi-region settings.json, set "
                "instances.availability_zone to a {region: az} mapping "
                "instead of a single string"
            )
            raise BenchError(
                'AZ resolution',
                Exception(
                    f"Configured availability_zone '{az}' is not in region "
                    f"'{region}' (an AZ name must start with its own region "
                    f"name). {hint}."
                )
            )

        if az is None:
            r = client.describe_availability_zones(
                Filters=[
                    {'Name': 'state', 'Values': ['available']},
                    # Exclude zones without a default subnet.
                    {'Name': 'zone-type', 'Values': ['availability-zone']},
                    {'Name': 'opt-in-status', 'Values': ['opt-in-not-required']},
                ]
            )
            zones = sorted(z['ZoneName'] for z in r['AvailabilityZones'])
            if not zones:
                raise BenchError(
                    'AZ resolution',
                    Exception(f'No available availability zone found in {region}')
                )
            az = zones[0]

        r = client.describe_subnets(
            Filters=[
                {'Name': 'availability-zone', 'Values': [az]},
                {'Name': 'default-for-az', 'Values': ['true']},
            ]
        )
        subnets = r.get('Subnets', [])
        if not subnets:
            raise BenchError(
                'Subnet resolution',
                Exception(f'No default-for-az subnet found in {az} ({region})')
            )
        return az, subnets[0]['SubnetId'], subnets[0]['VpcId']

    def _security_group_id(self, client, vpc_id):
        '''Return the security-group ID for `vpc_id`, failing on ambiguity.'''
        r = client.describe_security_groups(
            Filters=[
                {'Name': 'group-name', 'Values': [self.SECURITY_GROUP_NAME]},
                {'Name': 'vpc-id', 'Values': [vpc_id]},
            ]
        )
        groups = r.get('SecurityGroups', [])
        if not groups:
            raise BenchError(
                'Security group resolution',
                Exception(
                    f"Security group '{self.SECURITY_GROUP_NAME}' not found "
                    f"in VPC '{vpc_id}'"
                )
            )
        if len(groups) > 1:
            raise BenchError(
                'Security group resolution',
                Exception(
                    f"Multiple security groups named "
                    f"'{self.SECURITY_GROUP_NAME}' found in VPC '{vpc_id}' "
                    f"({[g['GroupId'] for g in groups]}); refusing to guess "
                    f"which one to use"
                )
            )
        return groups[0]['GroupId']

    # Canonical Ubuntu owner account.
    CANONICAL_OWNER_ID = '099720109477'

    def _get_ami(self, client):
        # Select the newest matching Ubuntu amd64 HVM/EBS image.
        response = client.describe_images(
            Owners=[self.CANONICAL_OWNER_ID],
            Filters=[
                {
                    'Name': 'name',
                    'Values': [
                        'ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*'
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
                Exception('No matching Ubuntu 24.04 AMI found in region')
            )
        return images[0]['ImageId']

    def create_instances(self, instances):
        assert isinstance(instances, int) and instances > 0

        # Create or update the security group in every region.
        for client in self.clients.values():
            try:
                self._create_security_group(client)
            except ClientError as e:
                raise BenchError('Failed to create security group', AWSError(e))

        try:
            # Create all instances.
            size = instances * len(self.clients)
            # Use one-time EC2 Spot requests when enabled.
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
            # Resolve each region's subnet once and reuse it for the collector.
            az_subnet_by_region = {}
            sg_id_by_region = {}
            progress = progress_bar(
                self.clients.items(), prefix=f'Creating {size} instances'
            )
            for region, client in progress:
                az, subnet_id, vpc_id = self._resolve_az_subnet(client, region)
                sg_id = self._security_group_id(client, vpc_id)
                az_subnet_by_region[region] = (az, subnet_id, vpc_id)
                sg_id_by_region[region] = sg_id
                client.run_instances(
                    ImageId=self._get_ami(client),
                    InstanceType=self.settings.instance_type,
                    KeyName=self.settings.key_name,
                    MaxCount=instances,
                    MinCount=instances,
                    SecurityGroupIds=[sg_id],
                    SubnetId=subnet_id,
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
            self._wait(['pending'], name=self.INSTANCE_NAME)
            Print.heading(f'Successfully created {size} new instances')

            # Create one metrics collector in the first configured region.
            region, client = next(iter(self.clients.items()))
            collector_type = getattr(
                self.settings, 'monitor_instance_type', None
            ) or self.settings.instance_type
            # Reuse the validator subnet so collector traffic stays intra-AZ.
            _, collector_subnet_id, _ = az_subnet_by_region[region]
            collector_sg_id = sg_id_by_region[region]
            Print.info(
                f'Creating the metrics-collector instance ({collector_type}, '
                f'{region})...'
            )
            client.run_instances(
                ImageId=self._get_ami(client),
                InstanceType=collector_type,
                KeyName=self.settings.key_name,
                MaxCount=1,
                MinCount=1,
                SecurityGroupIds=[collector_sg_id],
                SubnetId=collector_subnet_id,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{
                        'Key': 'Name',
                        'Value': self.COLLECTOR_NAME
                    }]
                }],
                EbsOptimized=True,
                BlockDeviceMappings=[{
                    'DeviceName': '/dev/sda1',
                    'Ebs': {
                        'VolumeType': 'gp2',
                        # The collector stores one benchmark run.
                        'VolumeSize': 50,
                        'DeleteOnTermination': True
                    }
                }],
                **spot_options,
            )
            Print.info('Waiting for the metrics-collector instance to boot...')
            self._wait(['pending'], name=self.COLLECTOR_NAME)
            Print.heading('Successfully created the metrics-collector instance')
        except ClientError as e:
            raise BenchError('Failed to create AWS instances', AWSError(e))

    def _spot_price(self, client, instance_type):
        '''Return the latest Spot price, or `None` if unavailable.'''
        try:
            r = client.describe_spot_price_history(
                InstanceTypes=[instance_type],
                ProductDescriptions=['Linux/UNIX'],
                MaxResults=1,
            )
        except ClientError:
            return None
        history = r.get('SpotPriceHistory', [])
        if not history:
            return None
        return float(history[0]['SpotPrice'])

    @staticmethod
    def _format_cost(total_usd, breakdown):
        lines = [
            'AWS cost estimate (alive-time x price; EXCLUDES EBS storage, '
            'EC2->internet egress, and in-use public IPv4 address charges '
            '-- see below; spot is billed per-second -- this is a close '
            'estimate, not the invoice):'
        ]
        if not breakdown:
            lines.append('  No pending/running instances.')
        for row in breakdown:
            price_note = ' [approx, on-demand fallback]' if row['approximate'] else ' [spot]'
            lines.append(
                f"  {row['region']:<12} {row['instance_type']:<12} "
                f"x{row['count']:<3} @ ${row['price_per_hour']:.4f}/hr{price_note} "
                f"-> {row['instance_hours']:.4f} instance-hours = "
                f"${row['subtotal_usd']:.4f}"
            )
        lines.append(f'  TOTAL: ${total_usd:.4f}')
        lines.append(
            '  Node<->node/collector transfer is genuinely free (private '
            'IPs, single-AZ pinning -- see create_instances/'
            '_resolve_az_subnet). NOT included above: EC2->internet egress '
            "for this harness's own per-run log downloads and metrics pulls "
            'over public IPs (remote.py), and in-use public IPv4 address '
            'charges (~$0.005/hr each).'
        )
        return '\n'.join(lines)

    def estimate_cost(self, states=('pending', 'running')):
        '''Estimate EC2 cost, excluding EBS, internet egress, and public IPv4 charges.'''
        now = datetime.now(timezone.utc)
        spot_price_cache = {}  # (region, instance_type) -> float or None
        # (region, instance_type, approximate) -> {'count', 'instance_hours', 'price'}
        aggregate = OrderedDict()

        for region, client in self.clients.items():
            try:
                r = client.describe_instances(
                    Filters=[
                        {
                            'Name': 'tag:Name',
                            'Values': [self.INSTANCE_NAME, self.COLLECTOR_NAME]
                        },
                        {
                            'Name': 'instance-state-name',
                            'Values': list(states)
                        },
                    ]
                )
            except ClientError as e:
                raise BenchError(
                    'Failed to describe instances for cost estimate', AWSError(e)
                )

            instances = [y for x in r['Reservations'] for y in x['Instances']]
            for inst in instances:
                instance_type = inst['InstanceType']
                launch_time = inst['LaunchTime']  # tz-aware UTC (boto3-provided)
                alive_hours = max((now - launch_time).total_seconds(), 0.0) / 3600
                is_spot = inst.get('InstanceLifecycle') == 'spot'

                price, approximate = None, False
                if is_spot:
                    cache_key = (region, instance_type)
                    if cache_key not in spot_price_cache:
                        spot_price_cache[cache_key] = self._spot_price(
                            client, instance_type
                        )
                    price = spot_price_cache[cache_key]
                if price is None:
                    price = self.ON_DEMAND_FALLBACK_PRICE.get(
                        instance_type, self.DEFAULT_FALLBACK_PRICE
                    )
                    approximate = True

                key = (region, instance_type, approximate)
                entry = aggregate.setdefault(
                    key, {'count': 0, 'instance_hours': 0.0, 'price': price}
                )
                entry['count'] += 1
                entry['instance_hours'] += alive_hours

        breakdown = []
        total_usd = 0.0
        for (region, instance_type, approximate), entry in aggregate.items():
            subtotal = entry['instance_hours'] * entry['price']
            total_usd += subtotal
            breakdown.append({
                'region': region,
                'instance_type': instance_type,
                'count': entry['count'],
                'price_per_hour': entry['price'],
                'approximate': approximate,
                'instance_hours': entry['instance_hours'],
                'subtotal_usd': subtotal,
            })

        return {
            'total_usd': total_usd,
            'breakdown': breakdown,
            'formatted': self._format_cost(total_usd, breakdown),
        }

    def terminate_instances(self):
        '''Terminate all validator and metrics-collector instances owned by this harness.'''
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
        '''Return validator public IPs for coordinator access; exclude the collector.'''
        try:
            pairs = self._paired_hosts(['pending', 'running'], name=self.INSTANCE_NAME)
            ips = {region: [pub for pub, _ in v] for region, v in pairs.items()}
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances IPs', AWSError(e))

    def internal_hosts(self, flat=False):
        '''Return validator private IPs for node and client traffic.'''
        try:
            pairs = self._paired_hosts(['pending', 'running'], name=self.INSTANCE_NAME)
            ips = {region: [priv for _, priv in v] for region, v in pairs.items()}
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances private IPs', AWSError(e))

    def collector_host(self):
        '''Return the collector's public/private IP pair, or `None` if unavailable.'''
        try:
            pairs = self._paired_hosts(['pending', 'running'], name=self.COLLECTOR_NAME)
        except ClientError as e:
            raise BenchError('Failed to gather metrics-collector IPs', AWSError(e))
        for region_pairs in pairs.values():
            if region_pairs:
                return region_pairs[0]
        return None

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for region, ips in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, ip in enumerate(ips):
                new_line = '\n' if (i+1) % 6 == 0 else ''
                text += f'{new_line} {i}\tssh -i {key} ubuntu@{ip}\n'

        collector = self.collector_host()
        collector_text = (
            f'\n Metrics collector:\n'
            f' \tssh -i {key} ubuntu@{collector[0]} '
            f'(private ip: {collector[1]}, Prometheus: http://{collector[0]}:'
            f'{self.MONITOR_PORT})\n'
        ) if collector is not None else '\n Metrics collector: none\n'

        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            f'{collector_text}'
            '----------------------------------------------------------------\n'
        )
