from gcbmwalltowall.component.tileable import Tileable

class Layer(Tileable):

    def __init__(self, path, attribute=None, lookup_table=None):
        self.path = path
        self.attribute = attribute
        self.lookup_table = lookup_table

    def to_tiler_layer(self, rule_manager):
        raise NotImplementedError()
