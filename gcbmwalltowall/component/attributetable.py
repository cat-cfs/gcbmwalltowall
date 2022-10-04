class AttributeTable:

    def __init__(self):
        raise RuntimeError("Interface only")

    @property
    def attributes(self):
        raise NotImplementedError()

    def get_unique_values(self, attributes=None):
        raise NotImplementedError()

    @property
    def to_tiler_args(self, attributes=None):
        raise NotImplementedError()
