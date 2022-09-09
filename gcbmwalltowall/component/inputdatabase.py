class InputDatabase:

    def __init__(self, aidb_path, yield_path, yield_interval):
        self.aidb_path = aidb_path
        self.yield_path = yield_path
        self.yield_interval = yield_interval

    def create(self, classifiers, output_path):
        raise NotImplementedError()

