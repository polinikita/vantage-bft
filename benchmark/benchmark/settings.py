# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    '''AWS benchmark settings.'''

    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, username='ubuntu', spot=False,
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
        self.aws_regions = regions
        self.username = username
        # AWS EC2 Spot requests are enabled only when `spot` is true.
        self.spot = spot
        # Optional instance type for the metrics collector.
        self.monitor_instance_type = monitor_instance_type
        # Repository slug for fetch-mode nightly releases.
        self.release_repo = release_repo
        # Optional region-to-AZ pinning. Missing entries use automatic selection.
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
