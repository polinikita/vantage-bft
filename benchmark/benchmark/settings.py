# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    ''' Cloud-provider-agnostic settings.

    `instance.py` (AWS/boto3) reads `aws_regions`; `gcp_instance.py` reads
    `gcp_zones` and `project_id`/`templates`. Both attributes are populated
    from the same `instances.regions` list so a single settings file works
    for whichever InstanceManager `benchmark/fabfile.py` and `remote.py`
    are wired to. `project_id`/`templates`/`username` are GCP/SSH-user
    conveniences with no AWS meaning beyond `username` (the SSH login,
    e.g. "ubuntu"); they default to None/[]/"ubuntu" so an AWS-only
    settings file doesn't need to carry GCP placeholders.
    '''

    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, project_id=None,
                 templates=None, username='ubuntu'):
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
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')