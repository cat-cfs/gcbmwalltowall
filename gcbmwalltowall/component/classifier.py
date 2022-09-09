from gcbmwalltowall.component.tileable import Tileable

class Classifier(Tileable):

    def __init__(self, layer, values_path=None, values_col=None, yield_col=None):
        self.layer = layer
        self.values_path = values_path
        self.values_col = values_col
        self.yield_col = yield_col

    def to_tiler_layer(self, rule_manager):
        raise NotImplementedError()
