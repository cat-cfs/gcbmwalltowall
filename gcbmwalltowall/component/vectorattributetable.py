import csv
import json
from ftfy import fix_encoding
from pathlib import Path
from tempfile import TemporaryDirectory
from mojadata.util import ogr
from mojadata.layer.attribute import Attribute
from mojadata.layer.filter.valuefilter import ValueFilter
from gcbmwalltowall.component.attributetable import AttributeTable

class VectorAttributeTable(AttributeTable):

    def __init__(self, layer_path, lookup_path=None):
        self._cached_data = None
        self.layer_path = Path(layer_path).absolute()
        self.lookup_path = Path(lookup_path).absolute() if lookup_path else None
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

    def to_tiler_args(self, attributes=None):
        selected_attributes = self._get_selected_attributes(attributes)

        filters = {
            attr: value for attr, value in selected_attributes.items()
            if value is not None
        } if isinstance(selected_attributes, dict) else {}

        return {
            "attributes": [
                Attribute(
                    attribute, substitutions=self._data.get(attribute),
                    filter=ValueFilter(filters[attribute]) if attribute in filters else None)
                for attribute in selected_attributes
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
        shp = ogr.Open(str(self.layer_path))
        lyr = shp.GetLayer(0)
        defn = lyr.GetLayerDefn()

        attribute_table = {}
        for i in range(defn.GetFieldCount()):
            attribute = defn.GetFieldDefn(i).GetName()
            unique_values = shp.ExecuteSQL(
                f"SELECT DISTINCT {attribute} FROM {self.layer_path.stem}")

            attribute_table[attribute] = [row.GetField(0) for row in unique_values]
            shp.ReleaseResultSet(unique_values)

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

    def _load_substitutions(self):
        if not self.lookup_path:
            return {}

        substitutions = csv.reader(open(str(self.lookup_path)))
        header = next(substitutions)
        substitution_table = {col: {} for col in header[::2]}
        for row in substitutions:
            for original_col in range(0, len(header), 2):
                replacement_col = original_col + 1
                original_value = row[original_col]
                replacement_value = row[replacement_col]
                if self._is_null(original_value) or self._is_null(replacement_value):
                    continue
                
                attribute = header[original_col]
                substitution_table[attribute][original_value] = replacement_value

        return substitution_table

    def _get_selected_attributes(self, attributes):
        return (
            [attributes] if isinstance(attributes, str)
            else attributes if attributes is not None
            else self.attributes
        )

    def _is_null(self, string):
        return not string or not isinstance(string, str) or string.isspace()
