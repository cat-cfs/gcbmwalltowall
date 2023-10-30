import pandas as pd
import json
import logging
from ftfy import fix_encoding
from pathlib import Path
from tempfile import TemporaryDirectory
from mojadata.util import ogr
from mojadata.layer.attribute import Attribute
from mojadata.layer.filter.valuefilter import ValueFilter
from pandas._libs import interval
from gcbmwalltowall.component.attributetable import AttributeTable

class VectorAttributeTable(AttributeTable):

    def __init__(self, layer_path, lookup_path=None, layer=None):
        self._cached_data = None
        self.layer_path = Path(layer_path).absolute()
        self.lookup_path = Path(lookup_path).absolute() if lookup_path else None
        self.layer = layer
        if not self.layer_path.exists():
            raise ValueError(f"{layer_path} not found")

    @property
    def attributes(self):
        return list(self._data.keys())

    def get_unique_values(self, attributes=None):
        selected_attributes = self._get_selected_attributes(attributes)

        return {
            attribute: list(self._data[attribute].values())
            for attribute in selected_attributes
        }

    def to_tiler_args(self, attributes=None, filters=None):
        selected_attributes = self._get_selected_attributes(attributes)
        tiler_attributes = (
            selected_attributes if isinstance(selected_attributes, dict)
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
                            original_values.get(attr, {}).get(user_filter_value, [user_filter_value])
                        )
                else:
                    attr_filter_values.extend(original_values.get(attr, {}).get(user_filter, [user_filter]))
                
                tiler_filters[attr] = attr_filter_values

        return {
            "attributes": [
                Attribute(
                    layer_attribute, tiler_attribute,
                    ValueFilter(tiler_filters[layer_attribute]) if layer_attribute in tiler_filters else None,
                    self._data.get(layer_attribute))
                for layer_attribute, tiler_attribute in tiler_attributes.items()
            ]
        }

    @property
    def _data(self):
        if self._cached_data is None:
            self._cached_data = {}
            substitutions = self._load_substitutions()
            attribute_table = self._extract_attribute_table()
            for attribute, values in attribute_table.items():
                self._cached_data[attribute] = {}
                for value in values:
                    self._cached_data[attribute][value] = (
                        substitutions.get(attribute, {})
                                     .get(value, value))
            
        return self._cached_data.copy()

    def _extract_attribute_table(self):
        ds = ogr.Open(str(self.layer_path))
        lyr = ds.GetLayerByName(self.layer) if self.layer else ds.GetLayer(0)
        defn = lyr.GetLayerDefn()
        ds_table = self.layer or self.layer_path.stem

        attribute_table = {}
        num_attributes = defn.GetFieldCount()
        for i in range(num_attributes):
            field_num = i + 1
            logging.info(f"  reading attribute table (field {field_num} / {num_attributes})")
            attribute = defn.GetFieldDefn(i).GetName()
            unique_values = ds.ExecuteSQL(f"SELECT DISTINCT {attribute} FROM {ds_table}")
            attribute_table[attribute] = [row.GetField(0) for row in unique_values]
            ds.ReleaseResultSet(unique_values)

        # Fix any unicode errors and ensure the final attribute values are UTF-8. 
        # This fixes cases where a shapefile has a bad encoding along with non-ASCII
        # characters, causing the attribute values to have either mangled characters
        # or an ASCII encoding when it should be UTF-8.
        with TemporaryDirectory() as tmp:
            tmp_path = str(Path(tmp).joinpath("attributes.json"))
            open(tmp_path, "w", encoding="utf8", errors="surrogateescape").write(
                json.dumps(attribute_table, ensure_ascii=False))

            tmp_txt = list(fix_encoding(open(tmp_path).read()))
            open(tmp_path, "w", encoding="utf8").writelines(tmp_txt)

            return json.loads(open(tmp_path, encoding="utf8").read())

    def _load_substitutions(self, invert=False):
        if not self.lookup_path:
            return {}

        substitutions = pd.read_csv(str(self.lookup_path), dtype=str)
        header = substitutions.columns
        substitution_table = {col: {} for col in header[::2]}
        for _, row in substitutions.iterrows():
            for original_col in range(0, len(header), 2):
                replacement_col = original_col + 1
                original_value = row[original_col]
                replacement_value = row[replacement_col]
                if self._is_null(original_value) or self._is_null(replacement_value):
                    continue
                
                attribute = header[original_col]
                if not invert:
                    substitution_table[attribute][original_value] = replacement_value
                else:
                    if not substitution_table[attribute].get(replacement_value):
                        substitution_table[attribute][replacement_value] = []

                    substitution_table[attribute][replacement_value].append(original_value)

        return substitution_table

    def _get_selected_attributes(self, attributes):
        return (
            [attributes] if isinstance(attributes, str)
            else attributes if attributes is not None
            else self.attributes
        )

    def _is_null(self, string):
        return not string or not isinstance(string, str) or string.isspace()
