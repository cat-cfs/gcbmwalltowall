from __future__ import annotations

from typing import Any

from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.configuration.configuration import Configuration


class CompositeProjectBuilder(ProjectBuilder):

    composite_builder_keys = {"builder", "type", "config_files"}

    @staticmethod
    def build(config: Configuration) -> Configuration:
        builder_config = config["builder"]

        for base_config_file in builder_config["config_files"]:
            base_config = Configuration.load(
                config.resolve(base_config_file), config.config_path
            )
            base_config = ProjectBuilder._update_relative_paths(
                base_config, base_config.config_path, config.config_path
            )

            config = CompositeProjectBuilder._merge(config, base_config)

        # Users can override or explicitly configure top-level items, or provide
        # extra values for items that are collections (i.e. layers, disturbances).
        config = CompositeProjectBuilder._merge(config, builder_config)

        return config

    @staticmethod
    def _merge(
        root_config: Configuration, extra_config: dict[str, Any]
    ) -> Configuration:
        for k, v in extra_config.items():
            if k in CompositeProjectBuilder.composite_builder_keys:
                continue

            if isinstance(v, dict):
                if k in root_config:
                    root_config[k].update(v)
                else:
                    root_config[k] = v
            else:
                root_config[k] = v

        return root_config
