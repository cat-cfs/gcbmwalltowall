import json
import site
import sys
from pathlib import Path

class Configuration(dict):

    def __init__(self, d, config_path, working_path=None):
        super().__init__(d)
        self.config_path = Path(config_path).absolute()
        self.working_path = Path(working_path or config_path).absolute()

    @property
    def recliner2gcbm_exe(self):
        user_settings = Path(site.USER_BASE, "Tools", "gcbmwalltowall", "settings.json")
        global_settings = Path(sys.prefix, "Tools", "gcbmwalltowall", "settings.json")

        recliner2gcbm_exe = None
        for config_path in (user_settings, global_settings):
            if config_path.is_file():
                recliner2gcbm_exe = Path(json.load(open(config_path)).get("recliner2gcbm_exe", ""))
                if recliner2gcbm_exe.is_file():
                    return recliner2gcbm_exe

        raise RuntimeError(
            "Recliner2GCBM.exe not found - please check configuration in either "
            f"{global_settings} or {user_settings}")

    def resolve(self, path=None):
        return self.config_path.joinpath(path)

    def resolve_working(self, path=None):
        return self.working_path.joinpath(path)

    def find_lookup_table(self, layer_path):
        layer_path = Path(layer_path).absolute()

        # First check if there's an override lookup table in the working dir,
        # then check if there's one in the config file dir, and finally check
        # if there's a lookup table with the original layer.
        for lookup_table in (
            self.working_path.joinpath(layer_path.with_suffix(".csv").name),
            self.config_path.joinpath(layer_path.with_suffix(".csv").name),
            layer_path.with_suffix(".csv")
        ):
            if lookup_table.exists():
                return lookup_table

        return None

    @classmethod
    def load(cls, config_path, working_path=None):
        config_path = Path(config_path)

        return cls(
            json.load(open(config_path, "r")),
            Path(config_path).absolute().parent,
            Path(working_path or config_path.absolute().parent))
