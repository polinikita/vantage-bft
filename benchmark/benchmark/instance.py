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
    # METRICS-COLLECTOR-PREP: the dedicated metrics-collector instance gets its own
    # tag:Name value so it is distinguishable from validators at a glance (AWS
    # console, `describe_instances`) and, critically, so hosts()/internal_hosts()
    # (which filter on INSTANCE_NAME only, see below) never surface it to
    # `_select_hosts`/`_select_hosts_config` -- the collector is not a committee
    # member and runs no node.
    COLLECTOR_NAME = 'metrics-collector'
    SECURITY_GROUP_NAME = 'dag'
    # Prometheus HTTP API port on the metrics-collector instance -- scraped targets
    # dial validators (see config.py's committee 'metrics' addresses); this is the
    # port the *collector itself* listens on, queried by the coordinator laptop
    # after a run (remote.py's fetch_collector_metrics).
    MONITOR_PORT = 9090

    # COST-ESTIMATE: static on-demand fallback price (USD/hr), used by
    # `estimate_cost()` whenever an instance isn't Spot, or is Spot but its
    # `describe_spot_price_history` lookup fails. Keyed by instance type;
    # `DEFAULT_FALLBACK_PRICE` covers any type without its own entry.
    # c5.xlarge/eu-west-1 on-demand was ~0.192 USD/hr at the time this was
    # written -- re-check https://aws.amazon.com/ec2/pricing/on-demand/ if
    # instance_type/region in settings.json changes; this is a documented
    # approximation, not a live price feed (Cost Explorer/CloudTrail are
    # denied for this IAM user).
    ON_DEMAND_FALLBACK_PRICE = {
        'c5.xlarge': 0.192,  # eu-west-1
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
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        #
        # `name`: a single tag:Name value to filter on (INSTANCE_NAME or
        # COLLECTOR_NAME). None (the default) matches BOTH -- every instance
        # this harness owns, validator or collector alike -- which is what
        # terminate_instances/_wait (teardown, boot-wait) want: the collector
        # must be included in both. hosts()/internal_hosts() pass
        # name=INSTANCE_NAME explicitly so the committee/host-selection logic
        # never sees the collector; collector_host() passes name=COLLECTOR_NAME.
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
        ''' Per-region list of (public_ip, private_ip) pairs for instances in
        `state` (optionally restricted to tag:Name == `name`, see `_get`),
        built from a single `_get()` snapshot so the two addresses stay tied
        to the same physical instance. Any instance missing either address is
        dropped (with a warning) rather than left to silently shift the
        pairing of every instance after it -- see `_get`. '''
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
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        while True:
            sleep(1)
            ids, _, _ = self._get(state, name)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_security_group(self, client):
        ''' Idempotent w.r.t. BOTH the group and its ingress rules.

        METRICS-COLLECTOR-STEP2: the observed bug was every
        `fab fetch-metrics` query to the collector's Prometheus HTTP API
        (coordinator -> collector:9090) timing out, while the per-run direct
        node scrape (coordinator/nodes -> each validator's own metrics port,
        within the "Dag port" range below) worked fine. The MONITOR_PORT rule
        below already asks for 0.0.0.0/0 -- so a *freshly created* group is
        fine. The actual bug is `create_security_group` being a create-only,
        run-once operation: `create_instances` swallows
        'InvalidGroup.Duplicate' and never revisits an ALREADY-EXISTING group
        (e.g. one created by an earlier `fab create` against an older
        revision of this file, before the MONITOR_PORT rule existed here) --
        that group keeps whatever rules it had at creation time forever,
        silently missing any rule added to the code since. The "Dag port"
        rule predates MONITOR_PORT, so it was already present on such a
        group; :9090 was not, hence exactly this symptom.

        Fix: always (whether the group is new or pre-existing) (re-)authorize
        every ingress rule below, one `authorize_security_group_ingress` call
        per rule rather than one call for all three -- AWS fails the ENTIRE
        call if ANY single permission in it is already present
        ('InvalidPermission.Duplicate'), so a combined call against a
        partially-provisioned pre-existing group would raise on the first
        already-present rule and never reach the missing one. Per-rule calls
        let each rule's own 'already there' be swallowed independently while
        any genuinely missing rule (like :9090 above) still gets added. '''
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
                # METRICS-COLLECTOR-PREP: the metrics-collector's Prometheus
                # HTTP API, queried by the coordinator laptop after a run
                # (remote.py's fetch_collector_metrics). Same shared security
                # group as the validators (simplicity: one group, one call
                # site) -- validators get this rule too but nothing listens
                # on their :9090, so it is inert there. 0.0.0.0/0, same
                # posture as the SSH/Dag-port rules above: this is a
                # throwaway testbed (`fab destroy` tears the group down with
                # it), so narrowing to the coordinator's own public IP would
                # add a resolve-my-IP dependency (and break the moment that
                # IP changes mid-campaign) for no real security benefit here.
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

    # Canonical's official AWS account id (stable across regions/time).
    CANONICAL_OWNER_ID = '099720109477'

    def _get_ami(self, client):
        # The AMI id changes per region and the old fixed build-date
        # description (2020-10-26 focal) is long deregistered, so resolve
        # the newest available Ubuntu amd64 HVM/EBS image by owner + name
        # glob instead of pinning an ImageId or exact date.
        #
        # Ubuntu 24.04 LTS (noble), not 22.04 (jammy): the fetch-binary deploy
        # path (remote.py's default, non-`--source-build`, mode) downloads a
        # `node`/`benchmark_client` built in this repo's Dockerfile against
        # `rust:1.95-bookworm` -- glibc 2.36. Jammy ships glibc 2.35, too old
        # to dynamically link that binary; Noble ships glibc 2.39. The name
        # glob matches both `hvm-ssd/` (pre-24.04 path) and the newer
        # `hvm-ssd-gp3/` Canonical publishes 24.04+ server images under.
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

        # Create (or, if it already existed, re-authorize the ingress rules
        # of) the security group in every region -- see
        # `_create_security_group`'s docstring: it now swallows both
        # 'InvalidGroup.Duplicate' and per-rule 'InvalidPermission.Duplicate'
        # itself, so any ClientError reaching here is a genuine failure.
        for client in self.clients.values():
            try:
                self._create_security_group(client)
            except ClientError as e:
                raise BenchError('Failed to create security group', AWSError(e))

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
            self._wait(['pending'], name=self.INSTANCE_NAME)
            Print.heading(f'Successfully created {size} new instances')

            # METRICS-COLLECTOR-PREP: one dedicated, extra metrics-collector
            # instance (not a validator, runs no node) -- in the FIRST configured
            # region only. A single collector regardless of how many regions
            # `settings.json` lists: scraping a validator over its PRIVATE ip from
            # a collector in a *different* region would need cross-region VPC
            # peering this harness never sets up (today's settings.json is
            # single-region, so this is exact there; a genuinely multi-region
            # campaign would need that peering for validator<->validator traffic
            # regardless of this feature). Tagged COLLECTOR_NAME (never
            # INSTANCE_NAME) so hosts()/internal_hosts() -- and therefore
            # `_select_hosts`/the committee -- never see it; only
            # terminate_instances (matches both tags) and collector_host()
            # (COLLECTOR_NAME only) do.
            region, client = next(iter(self.clients.items()))
            collector_type = getattr(
                self.settings, 'monitor_instance_type', None
            ) or self.settings.instance_type
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
                SecurityGroups=[self.SECURITY_GROUP_NAME],
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
                        # The collector only runs a single Prometheus container
                        # (scrape data for one benchmark run, not a fleet of
                        # them) -- far less storage than a validator's 200GB.
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
        ''' Latest Spot price (USD/hr, float) for `instance_type` in
        `client`'s region, or None if the lookup fails (no history returned,
        or an API error) -- callers fall back to a static on-demand estimate
        in that case (see `ON_DEMAND_FALLBACK_PRICE`).

        `MaxResults=1` relies on `describe_spot_price_history` returning
        entries most-recent-timestamp-first when no explicit StartTime/
        EndTime is given (the documented/observed default) -- this is a
        "latest price" read, not a historical query. '''
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
            'AWS cost estimate (alive-time x price; EXCLUDES EBS storage and '
            'data transfer; spot is billed per-second -- this is a close '
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
        return '\n'.join(lines)

    def estimate_cost(self, states=('pending', 'running')):
        ''' Deterministic AWS EC2 cost estimate for every instance this
        harness owns (validators AND the metrics-collector) currently in
        `states` (default: the ones a teardown is about to terminate) --
        computed entirely from `describe_instances`/
        `describe_spot_price_history` (both allowed for this IAM user), with
        NO Cost Explorer/CloudTrail calls (both denied).

        For each instance: alive_hours = (now(UTC) - LaunchTime) / 3600.
        Spot instances (`InstanceLifecycle == 'spot'`) get priced from the
        latest `describe_spot_price_history` entry for their (region,
        instance type), cached per (region, type) so each pair is queried at
        most once regardless of instance count. Non-spot instances, and spot
        instances whose price lookup fails, fall back to a static on-demand
        estimate (`ON_DEMAND_FALLBACK_PRICE`/`DEFAULT_FALLBACK_PRICE`) and are
        flagged `approximate=True`.

        EXCLUDES EBS volume and data-transfer costs (the latter is minimal
        here: node<->node traffic runs over private VPC IPs, see
        `internal_hosts()`). Spot is billed per-second, so `alive_hours`
        itself is exact -- the only approximation is the fallback price path.
        This must be called while instances are still up (LaunchTime is only
        visible pre-termination), i.e. BEFORE `terminate_instances()`.

        Returns:
          {
            'total_usd': float,
            'breakdown': [
              {
                'region': str, 'instance_type': str, 'count': int,
                'price_per_hour': float, 'approximate': bool,
                'instance_hours': float, 'subtotal_usd': float,
              }, ...
            ],
            'formatted': str,  # multi-line human-readable report
          }
        '''
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
        ''' Terminates every instance this harness owns -- validators AND the
        dedicated metrics-collector alike. `_get`'s default `name=None` matches
        both INSTANCE_NAME and COLLECTOR_NAME, so no separate collector-teardown
        step is needed here. '''
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
        (node<->node/client<->node): see `internal_hosts()`.

        Validators ONLY (tag:Name == INSTANCE_NAME) -- the dedicated
        metrics-collector instance (COLLECTOR_NAME) is deliberately excluded so
        `_select_hosts`/the committee never see it; use `collector_host()` to
        reach the collector itself. '''
        try:
            pairs = self._paired_hosts(['pending', 'running'], name=self.INSTANCE_NAME)
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
        call them in `run()`) rather than on a shared API response.

        Validators ONLY -- see `hosts()`'s same note re: the metrics-collector. '''
        try:
            pairs = self._paired_hosts(['pending', 'running'], name=self.INSTANCE_NAME)
            ips = {region: [priv for _, priv in v] for region, v in pairs.items()}
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError('Failed to gather instances private IPs', AWSError(e))

    def collector_host(self):
        ''' (public_ip, private_ip) of the dedicated metrics-collector instance
        (tag:Name == COLLECTOR_NAME), or None if it hasn't been created yet (an
        older testbed predating this feature, or the instance is still booting
        and lacks one of its two addresses -- see `_paired_hosts`).
        `create_instances` creates exactly one, in the first configured region,
        so the first match across regions is it. Public ip: the coordinator's
        SSH/deploy target for installing + running Prometheus, and later for
        querying its HTTP API. Private ip: informational here (the collector
        doesn't need to know its own address to scrape outward), but returned
        for symmetry/completeness. '''
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
