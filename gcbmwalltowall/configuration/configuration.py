import json
from pathlib import Path

class Configuration(dict):

    def __init__(self, d, config_path, working_path=None):
        super().__init__(d)
        self.config_path = Path(config_path).resolve()
        self.working_path = Path(working_path or config_path).resolve()

    def resolve(self, path):
        return self.config_path.joinpath(path).resolve()

    def resolve_working(self, path):
        return self.working_path.joinpath(path).resolve()

    @classmethod
    def load(cls, config_path):
        return cls(json.load(open(config_path, "r")), Path(config_path).resolve().parent)
