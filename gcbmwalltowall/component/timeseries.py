import fnmatch
import logging
import json
import sys
import csv
import numpy as np
import pandas as pd
from osgeo import gdal
from tempfile import TemporaryDirectory
from mojadata.util import ogr
from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.component.tileable import Tileable
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.util.yearparser import YearParser


class Timeseries(Tileable):

    def __init__(
        self,
        name,
        pattern,
        year=None,
        value=None,
        lookup_table=None,
        layers=None,
        **layer_kwargs,
    ):
        self.name = name
        self.pattern = Path(pattern)
        self.year = year
        self.value = value
        self.lookup_table = Path(lookup_table) if lookup_table else None
        self.layers = layers
        self.layer_kwargs = layer_kwargs or {}
        self._temp_dir = TemporaryDirectory()

    def to_tiler_layer(self, rule_manager, **kwargs):
        pattern_root = self.pattern.absolute().parent
        while "*" in pattern_root.name:
            pattern_root = pattern_root.parent

        if not pattern_root.exists():
            logging.fatal(
                f"Error scanning for timeseries layer pattern {self.pattern}: "
                f"parent directory {pattern_root} does not exist"
            )

            sys.exit("Fatal error preparing timeseries layers")

        timeseries_layers = []
        pattern_glob = str(self.pattern.relative_to(pattern_root))
        for layer_path in pattern_root.glob(pattern_glob):
            if layer_path.suffix == ".gdb" and self.layers:
                sublayers = self.layers
                if isinstance(sublayers, str):
                    ds = ogr.Open(str(layer_path))
                    sublayers = fnmatch.filter(
                        (ds.GetLayer(i).GetName() for i in range(ds.GetLayerCount())),
                        sublayers,
                    )

                    del ds

                for sublayer in sublayers:
                    layer_kwargs = self.layer_kwargs.copy()
                    layer_kwargs.update({"layer": sublayer})
                    timeseries_layers.extend(
                        self._to_tiler_layer(
                            layer_path, rule_manager, layer_kwargs, **kwargs
                        )
                    )
            else:
                timeseries_layers.extend(
                    self._to_tiler_layer(
                        layer_path, rule_manager, self.layer_kwargs, **kwargs
                    )
                )

        if not timeseries_layers:
            logging.fatal(
                f"Error scanning for timeseries layer pattern {self.pattern}: "
                f"no matching files found"
            )

            sys.exit("Fatal error preparing timeseries layers")

        return timeseries_layers

    def _to_tiler_layer(self, layer_path, rule_manager, layer_kwargs, **kwargs):
        timeseries_layers = []
        layer = Layer(
            self._make_tiler_name(layer_path, layer_kwargs.get("layer")),
            layer_path,
            lookup_table=self.lookup_table,
            tags=[f"timeseries:{self.name}"],
            **layer_kwargs,
        )

        attribute_table = layer.attribute_table
        year = self._get_year_or_attribute(layer_path, attribute_table)
        if layer.is_raster:
            value_attr = self.value or (
                next((a for a in attribute_table if a != year)) if attribute_table
                else None
            )

            # timeseries values come directly from pixel values if not in attribute table
            if not value_attr:
                lookup_table = Path(self._temp_dir.name).joinpath("attrs.csv")
                value_attr = "value"
                with gdal.Open(layer_path) as ds:
                    px_values = np.unique(ds.GetRasterBand(1).ReadAsArray().flatten())
                    px_values = px_values[px_values != ds.GetRasterBand(1).GetNoDataValue()]
                    lookup_data = pd.DataFrame({"px": px_values, "value": px_values})
                    lookup_data.to_csv(lookup_table, index=False)
                    layer.lookup_table = lookup_table
    
            if year not in attribute_table:
                lookup_table = Path(self._temp_dir.name).joinpath("attrs.csv")
                lookup_data = pd.read_csv(layer.lookup_table)
                lookup_data["year"] = int(float(year))
                lookup_data.to_csv(lookup_table, index=False)
                layer.lookup_table = lookup_table
                year = "year"

            layer.attributes = [year, value_attr]
            timeseries_layers.append(layer.to_tiler_layer(rule_manager, **kwargs))
        else:
            extended_attribute_table = None
            if year not in attribute_table:
                extended_attribute_data = []
                join_attr, join_values = next(iter(attribute_table.items()))
                for join_value in join_values:
                    extended_attribute_data.append({
                        join_attr: join_value,
                        "year": int(float(year)),
                    })

                extended_attribute_table = Path(self._temp_dir.name).joinpath("ext_attrs.csv")
                with open(extended_attribute_table, "w", newline="") as ext_attrs:
                    writer = csv.DictWriter(ext_attrs, fieldnames=[join_attr, "year"])
                    writer.writeheader()
                    writer.writerows(extended_attribute_data)

                year = "year"

            value_attr = self.value or next((a for a in attribute_table if a != year))
            tiler_attributes = {
                year: "year",
                value_attr: "value",
            }

            kwargs["raw"] = False
            timeseries_layers.append(
                Layer(
                    self._make_tiler_name(layer_path, layer_kwargs.get("layer")),
                    layer_path,
                    tiler_attributes,
                    lookup_table=self.lookup_table,
                    tags=[f"timeseries:{self.name}"],
                    extended_attribute_table=extended_attribute_table,
                ).to_tiler_layer(rule_manager, **kwargs)
            )

        return timeseries_layers

    def _make_tiler_name(self, layer_path, *args):
        # strip _moja in case of retiling previously tiled layers
        return "_".join([
            self.name,
            "ts",
            layer_path.stem.replace("_moja", ""),
            *(str(a) for a in args if a is not None)
        ])

    def _get_year_or_attribute(self, layer_path, attribute_table):
        logging.info(f"  checking for timeseries year in {layer_path.name}...")
        if self.year is not None and isinstance(self.year, str) and "filename" in self.year:
            parse_config = self.year.split(":")
            if len(parse_config) == 1:
                year = YearParser().try_parse_year(layer_path.name)
            else:
                try:
                    parse_config = json.loads(parse_config[1])
                except:
                    parse_config = parse_config[1]

                year = YearParser(parse_config).try_parse_year(layer_path.name)

            if year is None:
                raise RuntimeError(f"Year not parseable from filename in {layer_path}.")

            logging.info(f"  using year from filename: {year}")

            return year

        if self.year is not None:
            logging.info(f"  using configured value: {self.year}")
            return self.year

        # Check for the first attribute where all the unique values could be
        # interpreted as a timeseries year.
        candidates = []
        for attribute, values in attribute_table.items():
            if any((v is not None for v in values)) and all(
                (self._looks_like_year(v) for v in values if v is not None)
            ):
                logging.info(f"  found candidate attribute: {attribute}")
                candidates.append(attribute)
        
        if candidates:
            if len(candidates) > 1:
                raise RuntimeError(
                    "Multiple candidate attributes found for year of timeseries in "
                    f"{layer_path.name}: {candidates}. Attribute must be explicitly configured."
                )
            
            logging.info(f"  using attribute: {candidates[0]}")
            return candidates[0]

        # Then check if the timeseries year is parseable from the filename.
        year = YearParser().try_parse_year(layer_path.name)
        if year is None:
            raise RuntimeError(
                f"No disturbance year configured or found in {layer_path}."
            )

        logging.info(f"  using year from filename: {year}")

        return year

    def _looks_like_year(self, value):
        # If it parses to an int and has 4 digits, it's probably a year. We don't
        # try full date parsing here because there could be attributes with all
        # kinds of numeric values that aren't a year.
        try:
            val = int(float(value))
        except:
            return False

        if len(str(val)) != 4:
            return False

        if not (val > 1000 and val < 2500):
            return False

        return True
