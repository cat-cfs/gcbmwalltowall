import json
import site
import sys
from pathlib import Path

class Configuration(dict):

    def __init__(self, d, config_path, working_path=None):
        super().__init__(d)
        self.config_path = Path(config_path).resolve()
        self.working_path = Path(working_path or config_path).resolve()

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

    def resolve(self, path):
        return self.config_path.joinpath(path).resolve()

    def resolve_working(self, path):
        return self.working_path.joinpath(path).resolve()

    @classmethod
    def load(cls, config_path):
        return cls(json.load(open(config_path, "r")), Path(config_path).resolve().parent)
