from mojadata.boundingbox import BoundingBox as TilerBoundingBox
from gcbmwalltowall.component.tileable import Tileable

class BoundingBox(Tileable):

    def __init__(self, layer, resolution=0.001):
        self.layer = layer
        self.resolution = resolution or 0.001

    def to_tiler_layer(self, rule_manager, **kwargs):
        return TilerBoundingBox(self.layer.to_tiler_layer(rule_manager),
                                pixel_size=self.resolution)
