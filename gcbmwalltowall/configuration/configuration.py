import json
import site
import sys

import pandas as pd

from gcbmwalltowall.util.encoding import load_json
from gcbmwalltowall.util.path import Path


class Configuration(dict):

    global_settings_paths = [
        Path(sys.prefix, "Tools", "gcbmwalltowall", "settings.json"),
        Path(sys.prefix, "local", "Tools", "gcbmwalltowall", "settings.json"),
        Path(sys.prefix, "gcbmwalltowall", "settings.json"),
        Path(sys.prefix, "local", "gcbmwalltowall", "settings.json"),
        Path(site.USER_BASE, "Tools", "gcbmwalltowall", "settings.json"),
    ]

    def __init__(self, d, config_path, working_path=None):
        super().__init__(d)
        self.config_path = Path(config_path).absolute()
        self.working_path = Path(working_path or config_path).absolute()
        self._load_settings()

    @property
    def gcbm_exe(self):
        return self._find_file("gcbm_exe", "moja.cli.exe")

    @property
    def distributed_client(self):
        return self._find_file("distributed_client", "gcbm_client.py")

    @property
    def gcbm_disturbance_order(self):
        disturbance_order_file = self.gcbm_disturbance_order_path
        disturbance_order = (
            list(pd.read_csv(disturbance_order_file, sep="\0", header=None)[0])
            if disturbance_order_file
            else None
        )

        return disturbance_order

    @property
    def gcbm_disturbance_order_path(self):
        disturbance_order_path = self.get(
            "disturbance_order",
            next(self.config_path.glob("disturbance_order.*"), None),
        )

        if not disturbance_order_path:
            return None

        return self.resolve(disturbance_order_path)

    @property
    def gcbm_template_path(self):
        template_path = self.get("gcbm_config_templates")
        if template_path:
            template_path = self.resolve(self["gcbm_config_templates"])

        if not template_path or not template_path.exists():
            template_path = next(
                (
                    path
                    for path in (
                        self.resolve("templates"),
                        Path(
                            site.USER_BASE,
                            "Tools",
                            "gcbmwalltowall",
                            "templates",
                            "default",
                        ),
                        Path(
                            sys.prefix,
                            "gcbmwalltowall",
                            "templates",
                            "default",
                        ),
                        Path(
                            sys.prefix,
                            "Tools",
                            "gcbmwalltowall",
                            "templates",
                            "default",
                        ),
                        Path(
                            sys.prefix,
                            "local",
                            "Tools",
                            "gcbmwalltowall",
                            "templates",
                            "default",
                        ),
                    )
                    if path.exists()
                ),
                None,
            )

        if not template_path:
            raise RuntimeError("GCBM config file templates not found")

        return Path(template_path).absolute()

    @property
    def settings_keys(self):
        settings_keys = set()
        for config_path in Configuration.global_settings_paths:
            if config_path.is_file():
                settings_keys.update(json.load(open(config_path)).keys())

        return settings_keys

    def resolve(self, path=None):
        if path is not None:
            path = Path(path)

        return self.config_path.joinpath(path).resolve()

    def resolve_working(self, path=None):
        if path is not None:
            path = Path(path)

        return self.working_path.joinpath(path).resolve()

    def find_lookup_table(self, layer_path):
        layer_path = Path(layer_path).absolute()

        # First check if there's an override lookup table in the working dir,
        # then check if there's one in the config file dir, and finally check
        # if there's a lookup table with the original layer.
        for lookup_table in (
            self.working_path.joinpath(layer_path.with_suffix(".csv").name),
            self.config_path.joinpath(layer_path.with_suffix(".csv").name),
            layer_path.with_suffix(".csv"),
        ):
            if lookup_table.exists():
                return lookup_table

        return None

    def _load_settings(self):
        project_settings = self.copy()
        for config_path in Configuration.global_settings_paths:
            if config_path.is_file():
                self.update(json.load(open(config_path)))

        self.update(project_settings)

    def _find_file(self, setting_name, file_name):
        target_file = self.resolve(Path(self.get(setting_name, "")))
        if target_file.is_file():
            return target_file

        raise RuntimeError(
            f"{file_name} not found - please check {setting_name} setting in any of these locations: "
            + ", ".join(Configuration.global_settings_paths)
        )

    @classmethod
    def load(cls, config_path, working_path=None):
        config_path = Path(config_path).absolute()

        return cls(
            load_json(config_path),
            config_path.parent,
            Path(working_path or config_path.parent),
        )
