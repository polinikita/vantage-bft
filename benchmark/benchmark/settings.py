# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    '''Cloud-provider settings shared by AWS and GCP managers.'''

    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, project_id=None,
                 templates=None, username='ubuntu', spot=False,
                 monitor_instance_type=None, release_repo=None,
                 availability_zone=None):
        inputs_str = [
            key_name, key_path, repo_name, repo_url, branch, instance_type
        ]
        if isinstance(aws_regions, list):
            regions = aws_regions
        else:
            regions = [aws_regions]
        inputs_str += regions
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        ok &= isinstance(spot, bool)
        ok &= len(regions) > 0
        if not ok:
            raise SettingsError('Invalid settings types')

        self.key_name = key_name
        self.key_path = key_path

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        # Same list under both names: instance.py (AWS) reads aws_regions,
        # gcp_instance.py reads gcp_zones.
        self.aws_regions = regions
        self.gcp_zones = regions
        self.project_id = project_id
        self.templates = templates if templates is not None else []
        self.username = username
        # AWS EC2 Spot requests are enabled only when `spot` is true.
        self.spot = spot
        # Optional instance type for the metrics collector.
        self.monitor_instance_type = monitor_instance_type
        # Build-once/deploy-prebuilt-binary (fetch mode, remote.py's default
        # non-`--source-build` path): "<OWNER>/<REPO>" GitHub slug the
        # `docker.yml` workflow publishes the rolling `nightly` release to.
        # Optional -- None (absent from settings.json) means fetch mode has
        # no release to download from and `Bench._update` raises a clear
        # error telling the user to fill this in (or pass --source-build).
        self.release_repo = release_repo
        # Single-AZ pinning (instance.py's create_instances/_resolve_az_subnet):
        # the AWS availability zone every instance (validators AND the
        # metrics-collector) in a region launches into, so intra-committee
        # private-IP traffic stays intra-AZ (free) rather than merely
        # intra-region (still billed cross-AZ). Either a plain string (e.g.
        # "eu-west-1a"), applied to every region -- only sound for a
        # single-region settings.json, since an AZ name is region-scoped and
        # instance.py rejects a string that does not start with the region
        # it's being applied to -- or a {region: az} dict for a multi-region
        # settings.json (e.g. {"eu-west-1": "eu-west-1a"}), where a region
        # missing from the dict falls back to auto-pick for that region only.
        # Optional -- None (absent from settings.json, the default), or a
        # dict/region combination that resolves to no entry, makes
        # instance.py auto-pick the first 'available' ordinary AZ in that
        # region instead.
        self.availability_zone = availability_zone

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['key']['name'],
                data['key']['path'],
                data['port'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                data['instances']['type'],
                data['instances']['regions'],
                data.get('project_id'),
                data['instances'].get('templates'),
                data.get('username', 'ubuntu'),
                bool(data['instances'].get('spot', False)),
                data['instances'].get('monitor_type'),
                data['repo'].get('release_repo'),
                data['instances'].get('availability_zone'),
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
