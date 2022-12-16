import json
from os.path import relpath
from pathlib import Path
from gcbmwalltowall.configuration.configuration import Configuration

class ProjectBuilder:

    @staticmethod
    def get_builders():
        from gcbmwalltowall.builder.casfriprojectbuilder import CasfriProjectBuilder

        return {
            "casfri": CasfriProjectBuilder,
        }

    @staticmethod
    def build_from_file(config_path, output_path=None):
        config_path = Path(config_path).absolute()
        output_path = Path(output_path or config_path.parent).absolute()
        output_path.mkdir(parents=True, exist_ok=True)

        config = Configuration.load(config_path, output_path)
        builder_name = config.get("builder", {}).get("type")
        builder = ProjectBuilder.get_builders().get(builder_name)
        if builder:
            config = builder.build(config)
        elif builder_name:
            raise RuntimeError(
                f"Configuration file at {config_path} specified unknown builder "
                f"type '{builder_name}'")

        config = ProjectBuilder._update_relative_paths(
            config, Path(config_path).absolute().parent, output_path)

        output_file = output_path.joinpath(f"{config['project_name']}.json")
        include_builder_config = config_path == output_file
        ProjectBuilder._write_config(config, output_file, include_builder_config)

        return config

    @staticmethod
    def build(config):
        return config

    @staticmethod
    def _write_config(config, output_file, include_builder_config=True):
        settings_keys = config.settings_keys
        
        config = config.copy()
        if not include_builder_config:
            config.pop("builder", None)

        for key in settings_keys:
            config.pop(key)

        json.dump(config, open(output_file, "w", newline=""), indent=4)

    @staticmethod
    def _update_relative_paths(config, original_path, output_path):
        for k, v in config.items():
            if isinstance(v, dict):
                if k == "disturbances":
                    for dist_pattern, dist_config in v.copy().items():
                        dist_config = ProjectBuilder._update_relative_paths(
                            dist_config, original_path, output_path)

                        working_pattern = relpath(
                            original_path.joinpath(dist_pattern), output_path)

                        v[working_pattern] = dist_config
                        if working_pattern != dist_pattern:
                            del v[dist_pattern]
                else:
                    config[k] = ProjectBuilder._update_relative_paths(
                        v, original_path, output_path)
            else:
                if isinstance(v, str) and original_path.joinpath(v).exists():
                    config[k] = relpath(original_path.joinpath(v), output_path)

        return config
