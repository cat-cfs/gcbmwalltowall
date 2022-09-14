from pathlib import Path

class InputDatabase:

    def __init__(self, aidb_path, yield_path, yield_interval):
        self.aidb_path = Path(aidb_path)
        self.yield_path = Path(yield_path)
        self.yield_interval = yield_interval

    def create(self, classifiers, output_path):
        raise NotImplementedError()
