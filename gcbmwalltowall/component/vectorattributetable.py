from __future__ import annotations

import json
import logging
import sys
from multiprocessing import Pool
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from ftfy import fix_encoding, guess_bytes
from mojadata.layer.attribute import Attribute
from mojadata.layer.filter.valuefilter import ValueFilter
from mojadata.util import ogr

from gcbmwalltowall.component.attributetable import AttributeTable
from gcbmwalltowall.util.path import Path


class VectorAttributeTable(AttributeTable):

    _attribute_cache = {}
    _data_cache = {}

    def __init__(
        self,
        layer_path: Path | str,
        lookup_path: Path | str = None,
        layer: str = None,
        strict_lookup_table: bool = False,
    ):
        self.layer_path = Path(layer_path).absolute()
        self.lookup_path = Path(lookup_path).absolute() if lookup_path else None
        self.layer = layer
        self.strict_lookup_table = strict_lookup_table
        if not self.layer_path.exists():
            raise ValueError(f"{layer_path} not found")

    @property
    def attributes(self) -> list[str]:
        attributes = __class__._attribute_cache.get(self._cache_key)
        if attributes is None:
            ds = ogr.Open(str(self.layer_path))
            layer_id = self.layer if self.layer else 0
            lyr = ds.GetLayer(layer_id)
            if lyr is None:
                raise IOError(f"Error getting layer {layer_id} from {self.layer_path}")

            defn = lyr.GetLayerDefn()
            num_attributes = defn.GetFieldCount()
            attributes = [defn.GetFieldDefn(i).GetName() for i in range(num_attributes)]
            __class__._attribute_cache[self._cache_key] = attributes

        return attributes.copy()

    def get_unique_values(
        self, attributes: str | list[str] = None
    ) -> dict[str, list[Any]]:
        selected_attributes = self._get_selected_attributes(attributes)
        attribute_data = self._data(selected_attributes)

        return {
            attribute: list(attribute_data[attribute].values())
            for attribute in selected_attributes
        }

    def to_tiler_args(
        self,
        attributes: str | list[str] = None,
        filters: dict[str, Any | list[Any]] = None,
    ) -> dict[str, Any]:
        selected_attributes = self._get_selected_attributes(attributes)
        tiler_attributes = (
            selected_attributes
            if isinstance(selected_attributes, dict)
            else dict(zip(selected_attributes, selected_attributes))
        )

        # Tiler filters by original layer values, while gcbmwalltowall expects
        # users to filter by substituted values, if applicable.
        tiler_filters = {}
        if filters:
            original_values = self._load_substitutions(invert=True)
            for attr, user_filter in filters.items():
                attr_filter_values = []
                if isinstance(user_filter, list):
                    for user_filter_value in user_filter:
                        attr_filter_values.extend(
                            original_values.get(attr, {}).get(
                                user_filter_value, [user_filter_value]
                            )
                        )
                else:
                    attr_filter_values.extend(
                        original_values.get(attr, {}).get(user_filter, [user_filter])
                    )

                tiler_filters[attr] = attr_filter_values

        attribute_data = self._data(list(tiler_attributes.keys()))

        return {
            "attributes": [
                Attribute(
                    layer_attribute,
                    tiler_attribute,
                    (
                        ValueFilter(tiler_filters[layer_attribute])
                        if layer_attribute in tiler_filters
                        else None
                    ),
                    attribute_data.get(layer_attribute),
                    only_substitutions=self.strict_lookup_table,
                )
                for layer_attribute, tiler_attribute in tiler_attributes.items()
            ]
        }

    @property
    def _cache_key(self) -> tuple[Path, Path, str]:
        return (self.layer_path, self.lookup_path, self.layer)

    def _data(self, attributes: str | list[str] = None) -> dict[str, dict[Any, Any]]:
        cached_data = __class__._data_cache.get(self._cache_key, {})
        lazy_load_attributes = set(self._get_selected_attributes(attributes)) - set(
            cached_data.keys()
        )
        if not lazy_load_attributes:
            return cached_data.copy()

        substitutions = self._load_substitutions()
        attribute_table = self._extract_attribute_table(lazy_load_attributes)
        for attribute, values in attribute_table.items():
            cached_data[attribute] = {}
            for value in values:
                final_value = substitutions.get(attribute, {}).get(
                    str(value),
                    (
                        value
                        if not self.strict_lookup_table
                        or attribute not in substitutions
                        else None
                    ),
                )

                if final_value is not None:
                    cached_data[attribute][value] = final_value

        __class__._data_cache[self._cache_key] = cached_data

        return cached_data.copy()

    def _get_distinct_attribute_values(
        self, table: str, attribute: str
    ) -> tuple[str, list[Any]]:
        ds = ogr.Open(str(self.layer_path))
        query = ds.ExecuteSQL(
            f'SELECT DISTINCT "{attribute}" FROM {table} WHERE "{attribute}" IS NOT NULL'
        )
        unique_values = [row.GetField(0) for row in query]
        ds.ReleaseResultSet(query)

        return attribute, unique_values

    def _get_attribute_names(self, table: str) -> tuple[str, list[Any]]:
        ds = ogr.Open(str(self.layer_path))
        layer = ds.GetLayer(table)
        attributes = [field.GetName() for field in layer.schema]

        return attributes

    def _extract_attribute_table(self, attributes: list[str]) -> dict[str, list[Any]]:
        try:
            ogr.UseExceptions()
            ds = ogr.Open(str(self.layer_path))
            lyr = ds.GetLayerByName(self.layer) if self.layer else ds.GetLayer(0)
        except Exception as e:
            logging.fatal(e)
            sys.exit(f"Fatal error loading {self.layer_path}")
        finally:
            ogr.DontUseExceptions()

        ds_table = lyr.GetName()
        if ds is None or ds_table is None:
            err = f"Fatal error loading {self.layer_path}"
            logging.fatal(err)
            sys.exit(err)

        logging.info(f"  reading attribute table: {self.layer_path.stem} [{ds_table}]")

        layer_attributes = self._get_attribute_names(ds_table)
        missing_attributes = set(attributes).difference(set(layer_attributes))
        if missing_attributes:
            error = (
                f"Configuration specified attributes in {self.layer_path} ({self.layer or ''})"
                f" that do not exist: {', '.join(missing_attributes)}"
            )

            logging.fatal(error)
            raise RuntimeError(error)

        attribute_table = {}
        tasks = []
        with Pool() as pool:
            num_attributes = len(attributes)
            for i, attribute in enumerate(attributes):
                field_num = i + 1
                logging.info(f"    ({field_num} / {num_attributes}) {attribute}")
                tasks.append(
                    pool.apply_async(
                        self._get_distinct_attribute_values, (ds_table, attribute)
                    )
                )

            pool.close()
            pool.join()

        for task in tasks:
            attribute, unique_values = task.get()
            attribute_table[attribute] = unique_values

        # Fix any unicode errors and ensure the final attribute values are UTF-8.
        # This fixes cases where a shapefile has a bad encoding along with non-ASCII
        # characters, causing the attribute values to have either mangled characters
        # or an ASCII encoding when it should be UTF-8.
        with TemporaryDirectory() as tmp:
            tmp_path = str(Path(tmp).joinpath("attributes.json"))
            open(tmp_path, "w", encoding="utf8", errors="surrogateescape").write(
                json.dumps(attribute_table, ensure_ascii=False)
            )

            tmp_txt, _ = guess_bytes(open(tmp_path, "rb").read())
            open(tmp_path, "w", encoding="utf8").write(tmp_txt)

            return json.loads(fix_encoding(open(tmp_path).read()))

    def _load_substitutions(
        self, invert: bool = False
    ) -> dict[str, dict[str, list[Any]]]:
        if not self.lookup_path:
            return {}

        substitutions = pd.read_csv(str(self.lookup_path), dtype=str)
        header = substitutions.columns
        substitution_table = {col: {} for col in header[::2]}
        for _, row in substitutions.iterrows():
            for original_col in range(0, len(header), 2):
                replacement_col = original_col + 1
                original_value = row.iloc[original_col]
                replacement_value = row.iloc[replacement_col]
                if self._is_null(original_value) or self._is_null(replacement_value):
                    continue

                attribute = header[original_col]
                if not invert:
                    substitution_table[attribute][original_value] = replacement_value
                else:
                    if not substitution_table[attribute].get(replacement_value):
                        substitution_table[attribute][replacement_value] = []

                    substitution_table[attribute][replacement_value].append(
                        original_value
                    )

        return substitution_table

    def _get_selected_attributes(self, attributes: str | list[str]) -> list[str]:
        return (
            [attributes]
            if isinstance(attributes, str)
            else attributes if attributes is not None else self.attributes
        )

    def _is_null(self, string: str) -> bool:
        return not string or not isinstance(string, str) or string.isspace()
