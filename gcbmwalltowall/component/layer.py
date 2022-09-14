from pathlib import Path
from mojadata.layer.rasterlayer import RasterLayer
from mojadata.layer.vectorlayer import VectorLayer
from mojadata.layer.attribute import Attribute
from gcbmwalltowall.component.tileable import Tileable

class Layer(Tileable):

    raster_formats = [".tif", ".tiff"]
    vector_formats = [".shp", ".gdb"]

    def __init__(self, name, path, attribute=None, lookup_table=None):
        self.name = name
        self.path = Path(path)
        self.attribute = attribute
        self.lookup_table = lookup_table

    def to_tiler_layer(self, rule_manager, **kwargs):
        if self.path.suffix in Layer.raster_formats:
            return RasterLayer(str(self.path.resolve()), name=self.name, **kwargs)
        
        attribute = self.attribute or self.name

        return VectorLayer(self.name, str(self.path.resolve()), Attribute(attribute), **kwargs)
