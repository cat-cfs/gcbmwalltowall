from __future__ import annotations

from gcbmwalltowall.component.classifier import Classifier
from gcbmwalltowall.component.layer import Layer


class Cohort:

    def __init__(
        self, layers: list[Layer] = None, classifiers: list[Classifier] = None
    ):
        self.layers = layers
        self.classifiers = classifiers
        self._validate()

    def _validate(self):
        layer_names = [l.name for l in self.layers]
        if "cohort_proportion" not in layer_names:
            raise RuntimeError("Each cohort requires a cohort_proportion layer")
