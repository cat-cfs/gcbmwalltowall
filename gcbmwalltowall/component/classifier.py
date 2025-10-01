from numbers import Number

import pandas as pd
from mojadata.layer.dummylayer import DummyLayer

from gcbmwalltowall.component.tileable import Tileable
from gcbmwalltowall.util.path import Path


class Classifier(Tileable):

    def __init__(self, layer, values_path=None, values_col=None, yield_col=None):
        self.layer = layer
        self.values_path = Path(values_path) if values_path else None
        self.values_col = values_col
        self.yield_col = yield_col

    @property
    def name(self):
        return self.layer.name

    @property
    def values(self):
        values_col_idx = self._find_values_col_index()
        unique_values = set(
            pd.read_csv(self.values_path).iloc[:, values_col_idx].unique()
        )

        return unique_values

    @property
    def is_default(self):
        return False

    def to_tiler_layer(self, rule_manager, **kwargs):
        if self.layer.is_vector:
            kwargs["raw"] = False

        return self.layer.to_tiler_layer(rule_manager, tags=["classifier"], **kwargs)

    def _find_values_col_index(self):
        if isinstance(self.values_col, Number):
            return self.values_col

        classifier_data = pd.read_csv(self.values_path)
        if self.values_col:
            return classifier_data.columns.get_loc(self.values_col)

        if len(classifier_data.columns) == 1:
            return 0

        if self.name in classifier_data:
            return classifier_data.columns.get_loc(self.name)

        # No configured column or easy defaults - try to detect based on values.
        spatial_data = self.layer.attribute_table
        spatial_attribute = (
            next(iter(spatial_data.keys()))
            if len(spatial_data.keys()) == 1
            else (
                self.layer.attributes[0]
                if self.layer.attributes
                else (
                    self.name
                    if self.name in spatial_data
                    else next(iter(spatial_data.keys()))
                )
            )
        )

        if spatial_attribute in classifier_data:
            return classifier_data.columns.get_loc(spatial_attribute)

        spatial_classifier_values = {str(v) for v in spatial_data[spatial_attribute]}
        for col in classifier_data.columns:
            # The set of classifier values being imported into the database
            # doesn't have to include all values in the spatial layer, nor does
            # it have to be limited to only the values in the layer, so we look
            # for a column with any overlap.
            col_values = {str(v) for v in classifier_data[col].unique()}
            if not col_values.isdisjoint(spatial_classifier_values):
                return classifier_data.columns.get_loc(col)

        # As a last resort, maybe this classifier is completely wildcarded. See
        # if there's a column which is all wildcards.
        for col in classifier_data.columns:
            col_values = set(classifier_data[col].unique())
            if col_values == {"?"}:
                return classifier_data.columns.get_loc(col)

        raise RuntimeError(
            f"Unable to find column in {self.values_path} matching "
            f"{spatial_attribute} in {self.layer.path}"
        )


class DefaultClassifier(Classifier):

    def __init__(
        self,
        name,
        default_value=None,
        values_path=None,
        values_col=None,
        yield_col=None,
    ):
        self._name = name
        self._default_value = default_value
        self.values_path = Path(values_path) if values_path else None
        self.values_col = values_col
        self.yield_col = yield_col

    @property
    def name(self):
        return self._name

    @property
    def values(self):
        if self._default_value is None:
            return {"?"}

        return {"?", self._default_value}

    @property
    def is_default(self):
        return True

    def to_tiler_layer(self, rule_manager, **kwargs):
        return DummyLayer(
            self._name, self._default_value, tags=["classifier"], **kwargs
        )
