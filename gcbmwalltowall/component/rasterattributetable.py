import pandas as pd
from pathlib import Path
from gcbmwalltowall.component.attributetable import AttributeTable

class RasterAttributeTable(AttributeTable):

    def __init__(self, path):
        self._cached_data = None
        self.path = Path(path).resolve()
        if not self.path.exists():
            raise ValueError(f"{path} not found")

    @property
    def attributes(self):
        return list(self._data.columns[1:])

    def get_unique_values(self, attributes=None):
        selected_attributes = self._get_selected_attributes(attributes)

        return {
            attribute: list(self._data[attribute].unique())
            for attribute in selected_attributes
        }

    def to_tiler_args(self, attributes=None):
        selected_attributes = self._get_selected_attributes(attributes)

        return {
            "attributes": selected_attributes,
            "attribute_table": {
                row[0]: row[1:] for row in zip(
                    self._data.iloc[:, 0],
                    *[self._data[attribute] for attribute in selected_attributes]
                )
            }
        }

    @property
    def _data(self):
        if self._cached_data is None:
            self._cached_data = pd.read_csv(str(self.path))

        return self._cached_data.copy()

    def _get_selected_attributes(self, attributes):
        return (
            [attributes] if isinstance(attributes, str)
            else attributes if attributes is not None
            else self.attributes
        )
