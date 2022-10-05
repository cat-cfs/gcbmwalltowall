from mojadata.layer.rasterlayer import RasterLayer
from mojadata.layer.vectorlayer import VectorLayer
from mojadata.layer.attribute import Attribute
import pandas as pd
from mojadata.util import ogr
from pathlib import Path
from gcbmwalltowall.component.tileable import Tileable
from gcbmwalltowall.component.rasterattributetable import RasterAttributeTable
from gcbmwalltowall.component.vectorattributetable import VectorAttributeTable

class Layer(Tileable):

    raster_formats = [".tif", ".tiff"]
    vector_formats = [".shp", ".gdb"]

    def __init__(self, name, path, attributes=None, lookup_table=None):
        self.name = name
        self.path = Path(path)
        self.attributes = [attributes] if isinstance(attributes, str) else attributes
        self.lookup_table = Path(lookup_table) if lookup_table else None

    @property
    def attribute_table(self):
        attribute_table = self._load_lookup_table()
        if attribute_table is None:
            return {}
        
        return attribute_table.get_unique_values()

    @property
    def is_vector(self):
        return self.path.suffix in Layer.vector_formats

    @property
    def is_raster(self):
        return self.path.suffix in Layer.raster_formats

    def to_tiler_layer(self, rule_manager, **kwargs):
        lookup_table = self._load_lookup_table()
        if self.is_raster:
            return RasterLayer(
                str(self.path.resolve()),
                name=self.name,
                **lookup_table.to_tiler_args(self.attributes) if lookup_table else {},
                **kwargs)
        
        attributes = self.attributes or [
            self.name if self.name in lookup_table.attributes
            else lookup_table.attributes[0]
        ]

        # If it's the only selected attribute in a vector layer, and all the unique
        # values are numeric, then use raw mode (no attribute table in tiled output)
        # unless explicitly configured otherwise.
        if len(attributes) == 1 and lookup_table.is_numeric(next(iter(attributes))):
            kwargs["raw"] = kwargs.get("raw", True)

        return VectorLayer(
            self.name,
            str(self.path.resolve()),
            **lookup_table.to_tiler_args(attributes),
            **kwargs)

    def _find_lookup_table(self):
        lookup_table = Path(self.path.with_suffix(".csv"))
        return lookup_table if lookup_table.exists() else None

    def _load_lookup_table(self):
        lookup_table = self.lookup_table or self._find_lookup_table()
        if self.path.suffix in Layer.raster_formats:
            return RasterAttributeTable(lookup_table) if lookup_table else None

        return VectorAttributeTable(self.path, lookup_table)
