from __future__ import annotations

import json

from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.util.path import Path, relpath


class ProjectBuilder:

    @staticmethod
    def get_builders() -> dict[str, ProjectBuilder]:
        from gcbmwalltowall.builder.casfriprojectbuilder import \
            CasfriProjectBuilder
        from gcbmwalltowall.builder.compositeprojectbuilder import \
            CompositeProjectBuilder

        return {
            "casfri": CasfriProjectBuilder,
            "composite": CompositeProjectBuilder,
        }

    @staticmethod
    def build_from_file(
        config_path: str | Path, output_path: str | Path = None
    ) -> Configuration:
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
                f"type '{builder_name}'"
            )

        config = ProjectBuilder._update_relative_paths(
            config, Path(config_path).absolute().parent, output_path
        )

        output_file = output_path.joinpath(f"{config['project_name']}.json")
        include_builder_config = config_path == output_file
        ProjectBuilder._write_config(config, output_file, include_builder_config)

        return config

    @staticmethod
    def build(config: Configuration) -> Configuration:
        return config

    @staticmethod
    def _write_config(
        config: Configuration,
        output_file: Path | str,
        include_builder_config: bool = True,
    ):
        settings_keys = config.settings_keys

        config = config.copy()
        if not include_builder_config:
            config.pop("builder", None)

        for key in settings_keys:
            config.pop(key)

        json.dump(config, open(output_file, "w", newline=""), indent=4)

    @staticmethod
    def _update_relative_paths(
        config: Configuration, original_path: Path | str, output_path: Path | str
    ) -> Configuration:
        for k, v in config.items():
            if k in getattr(config, "settings_keys", []):
                continue

            if isinstance(v, dict):
                if k == "disturbances":
                    for dist_pattern, dist_config in v.copy().items():
                        if (
                            "*" in dist_config
                            or original_path.joinpath(Path(f"{dist_config}")).exists()
                        ):
                            v[dist_pattern] = relpath(
                                original_path.joinpath(dist_config), output_path
                            )
                        elif "pattern" in dist_config:
                            dist_config = ProjectBuilder._update_relative_paths(
                                dist_config, original_path, output_path
                            )

                            v[dist_pattern] = dist_config
                        else:
                            dist_config = ProjectBuilder._update_relative_paths(
                                dist_config, original_path, output_path
                            )

                            working_pattern = relpath(
                                original_path.joinpath(dist_pattern), output_path
                            )

                            v[working_pattern] = dist_config
                            if working_pattern != dist_pattern:
                                del v[dist_pattern]
                else:
                    config[k] = ProjectBuilder._update_relative_paths(
                        v, original_path, output_path
                    )
            else:
                if isinstance(v, str) and original_path.joinpath(Path(v)).exists():
                    config[k] = relpath(original_path.joinpath(v), output_path)

        return config
