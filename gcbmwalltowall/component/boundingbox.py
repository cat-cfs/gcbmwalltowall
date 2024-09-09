from __future__ import annotations
from mojadata.boundingbox import BoundingBox as TilerBoundingBox
from gcbmwalltowall.component.tileable import Tileable

class BoundingBox(Tileable):

    def __init__(self, layer: Layer, resolution: float = 0.001):
        self.layer = layer
        self.resolution = resolution or 0.001

    def to_tiler_layer(self, rule_manager: TransitionRuleManager, **kwargs: Any) -> Any:
        return TilerBoundingBox(self.layer.to_tiler_layer(rule_manager, **kwargs),
                                pixel_size=self.resolution)
