from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from gcbmwalltowall.component.attributetable import AttributeTable
from gcbmwalltowall.util.path import Path


class RasterAttributeTable(AttributeTable):

    def __init__(self, path: str | Path):
        self._cached_data = None
        self.path = Path(path).absolute()
        if not self.path.exists():
            raise ValueError(f"{path} not found")

    @property
    def attributes(self) -> list[str]:
        return list(self._data.columns[1:])

    def get_unique_values(
        self, attributes: str | list[str] = None
    ) -> dict[str, list[Any]]:
        selected_attributes = self._get_selected_attributes(attributes)

        return {
            attribute: list(self._data[attribute].unique())
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
            if isinstance(attributes, dict)
            else dict(zip(selected_attributes, selected_attributes))
        )

        return {
            "attributes": list(tiler_attributes.values()),
            "attribute_table": {
                row[0]: row[1:]
                for row in zip(
                    self._data.iloc[:, 0],
                    *[self._data[attribute] for attribute in tiler_attributes],
                )
            },
        }

    @property
    def _data(self) -> pd.DataFrame:
        if self._cached_data is None:
            self._cached_data = pd.read_csv(str(self.path)).replace(np.nan, None)

        return self._cached_data.copy()

    def _get_selected_attributes(self, attributes: str | list[str]) -> list[str]:
        return (
            [attributes]
            if isinstance(attributes, str)
            else attributes if attributes is not None else self.attributes
        )
