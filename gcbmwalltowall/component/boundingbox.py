from __future__ import annotations

from typing import Any

from mojadata.boundingbox import BoundingBox as TilerBoundingBox

from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.component.tileable import Tileable


class BoundingBox(Tileable):

    def __init__(self, layer: Layer, epsg: int = 4326, resolution: float = 0.001):
        self.layer = layer
        self.epsg = epsg or 4326
        self.resolution = resolution or 0.001

    def to_tiler_layer(self, rule_manager: TransitionRuleManager, **kwargs: Any) -> Any:
        return TilerBoundingBox(
            self.layer.to_tiler_layer(rule_manager, **kwargs),
            epsg=self.epsg,
            pixel_size=self.resolution,
            shrink_to_data=self.layer.is_raster,
        )
