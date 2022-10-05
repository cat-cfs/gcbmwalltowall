from numbers import Number

class AttributeTable:

    def __init__(self):
        raise RuntimeError("Interface only")

    @property
    def attributes(self):
        raise NotImplementedError()

    @property
    def to_tiler_args(self, attributes=None):
        raise NotImplementedError()

    def get_unique_values(self, attributes=None):
        raise NotImplementedError()

    def is_numeric(self, attribute):
        return all((
            isinstance(v, Number)
            for v in self.get_unique_values(attribute)[attribute]))
