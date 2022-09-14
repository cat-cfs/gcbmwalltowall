from pathlib import Path
from gcbmwalltowall.component.tileable import Tileable

class Classifier(Tileable):

    def __init__(self, layer, values_path=None, values_col=None, yield_col=None):
        self.layer = layer
        self.values_path = Path(values_path) if values_path else None
        self.values_col = values_col
        self.yield_col = yield_col

    def to_tiler_layer(self, rule_manager, **kwargs):
        return self.layer.to_tiler_layer(rule_manager, tags=["classifier"])
