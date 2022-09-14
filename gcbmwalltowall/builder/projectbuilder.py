import json
from pathlib import Path
from gcbmwalltowall.configuration.configuration import Configuration

class ProjectBuilder:

    builders = {
        "casfri": CasfriProjectBuilder,
    }

    @staticmethod
    def build_from_file(config_path, output_path=None):
        project_working_path = Path(output_path or config_path).parent()
        project_working_path.mkdir(parents=True, exist_ok=True)

        config = Configuration.load(config_path, project_working_path)
        builder_name = config["helper"].get("type") if "builder" in config else None
        builder = builders.get(builder_name)
        if builder:
            config = builder.build(config, project_working_path)
        elif builder_name:
            raise RuntimeError(
                f"Configuration file at {config_path} specified unknown builder "
                f"type '{helper_name}'")

        include_builder_config = not output_path or config_path == output_path
        ProjectBuilder._write_config(config, output_path or config_path, include_builder_config)

        return config

    @staticmethod
    def build(config, working_path):
        return config

    @staticmethod
    def _write_config(config, output_path, include_builder_config=True):
        if not include_builder_config:
            config = config.copy()
            config.pop("builder", None)

        json.dump(config, open(output_path, "w", newline=""), indent=4)
