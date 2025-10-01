from mojadata.layer.dummylayer import DummyLayer
from mojadata.layer.rasterlayer import RasterLayer
from mojadata.layer.vectorlayer import VectorLayer

from gcbmwalltowall.component.rasterattributetable import RasterAttributeTable
from gcbmwalltowall.component.tileable import Tileable
from gcbmwalltowall.component.vectorattributetable import VectorAttributeTable
from gcbmwalltowall.util.path import Path


class Layer(Tileable):

    raster_formats = [".tif", ".tiff"]
    vector_formats = [".shp", ".gdb"]

    def __init__(
        self,
        name,
        path,
        attributes=None,
        lookup_table=None,
        filters=None,
        layer=None,
        strict_lookup_table=False,
        **tiler_kwargs,
    ):
        self.name = name
        self.path = Path(path).absolute()
        self.attributes = [attributes] if isinstance(attributes, str) else attributes
        self.lookup_table = Path(lookup_table) if lookup_table else None
        self.filters = filters or {}
        self.layer = layer or tiler_kwargs.pop("layer_name", None)
        self.strict_lookup_table = strict_lookup_table
        self.tiler_kwargs = tiler_kwargs
        self._cached_lookup_table = None

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
        kwargs.update(self.tiler_kwargs)
        lookup_table = self._load_lookup_table()
        if self.is_raster:
            return RasterLayer(
                str(self.path.absolute()),
                name=self.name,
                **lookup_table.to_tiler_args(self.attributes) if lookup_table else {},
                **kwargs,
            )

        attributes = self.attributes or [
            (
                self.name
                if self.name in lookup_table.attributes
                else lookup_table.attributes[0]
            )
        ]

        # If it's the only selected attribute in a vector layer, and all the unique
        # values are numeric, then use raw mode (no attribute table in tiled output)
        # unless explicitly configured otherwise.
        if len(attributes) == 1 and lookup_table.is_numeric(next(iter(attributes))):
            kwargs["raw"] = kwargs.get("raw", True)

        return VectorLayer(
            self.name,
            str(self.path.absolute()),
            **lookup_table.to_tiler_args(attributes, self.filters),
            layer=self.layer,
            **kwargs,
        )

    def split(self, name=None, attributes=None, filters=None):
        layer_copy = __class__(
            name or self.name,
            self.path,
            attributes or self.attributes,
            self.lookup_table,
            filters or self.filters,
            self.layer,
            **self.tiler_kwargs,
        )

        layer_copy._cached_lookup_table = self._cached_lookup_table

        return layer_copy

    def _find_lookup_table(self):
        if not self.lookup_table:
            return None

        if self.lookup_table and not self.lookup_table.is_dir():
            if not self.lookup_table.exists():
                raise RuntimeError(f"{self.lookup_table} not found")

            return self.lookup_table

        # First check if the lookup table is specified as a directory, then see
        # if there's a lookup table for this layer inside.
        lookup_table = self.lookup_table.joinpath(self.path.with_suffix(".csv").name)
        if not lookup_table.exists():
            # Then check if there's a lookup table with the original layer.
            lookup_table = Path(self.path.with_suffix(".csv"))

        return lookup_table if lookup_table.exists() else None

    def _load_lookup_table(self):
        if self._cached_lookup_table is not None:
            return self._cached_lookup_table

        lookup_table = self._find_lookup_table()
        if self.path.suffix in Layer.raster_formats:
            self._cached_lookup_table = (
                RasterAttributeTable(lookup_table) if lookup_table else None
            )
        else:
            self._cached_lookup_table = VectorAttributeTable(
                self.path,
                lookup_table,
                layer=self.layer,
                strict_lookup_table=self.strict_lookup_table,
            )

        return self._cached_lookup_table


class DefaultLayer(Layer):

    def __init__(self, name, default_value):
        self.name = name
        self._default_value = default_value

    def to_tiler_layer(self, rule_manager, **kwargs):
        return DummyLayer(self.name, self._default_value, **kwargs)
