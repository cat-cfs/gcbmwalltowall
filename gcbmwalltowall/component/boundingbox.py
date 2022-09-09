from gcbmwalltowall.component.tileable import Tileable

class BoundingBox(Tileable):

    def __init__(self, layer, resolution=0.001):
        self.layer = layer
        self.resolution = resolution

    def to_tiler_layer(self, rule_manager):
        raise NotImplementedError()
