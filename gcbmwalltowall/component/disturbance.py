import re
from pathlib import Path
from mojadata.layer.attribute import Attribute
from mojadata.layer.filter.valuefilter import ValueFilter
from mojadata.layer.gcbm.disturbancelayer import DisturbanceLayer
from mojadata.layer.gcbm.transitionrule import TransitionRule
from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.component.tileable import Tileable

class Disturbance(Tileable):

    def __init__(
        self, pattern, input_db, year=None, disturbance_type=None, age_after=None,
        regen_delay=None, transition=None, lookup_table_dir=None, **layer_kwargs
    ):
        self.pattern = Path(pattern)
        self.input_db = input_db
        self.year = year
        self.disturbance_type = disturbance_type
        self.age_after = age_after
        self.regen_delay = regen_delay
        self.transition = transition
        self.lookup_table_dir = Path(lookup_table_dir) if lookup_table_dir else None
        self.layer_kwargs = layer_kwargs or {}

    def to_tiler_layer(self, rule_manager, **kwargs):
        disturbance_layers = []
        for layer_path in self.pattern.absolute().parent.glob(self.pattern.name):
            layer = Layer(
                layer_path.stem, layer_path, lookup_table=self.lookup_table_dir,
                **self.layer_kwargs)

            attribute_table = layer.attribute_table

            transition_rule = None
            spatial_classifier_transition = None
            age_after = self.get_configured_or_default(attribute_table, "reset_age", self.age_after)
            regen_delay = self.get_configured_or_default(attribute_table, "regen_delay", self.regen_delay)
            if age_after is not None:
                if regen_delay is None:
                    regen_delay = 0

                if self.transition:
                    if all((v in attribute_table for v in self.transition.values())):
                        spatial_classifier_transition = list(self.transition.keys())

                transition_rule = TransitionRule(
                    Attribute(regen_delay) if regen_delay in attribute_table else regen_delay,
                    Attribute(age_after) if age_after in attribute_table else age_after,
                    spatial_classifier_transition or self.transition)

            disturbance_type = self._get_disturbance_type_or_attribute(layer_path, attribute_table)
            year = self._get_disturbance_year_or_attribute(layer_path, attribute_table)

            if layer.is_raster:
                disturbance_layer = layer
                if spatial_classifier_transition:
                    # Transition is configured as classifier to layer attribute;
                    # use the inverse to rename those attributes to match the
                    # classifiers when tiling.
                    attributes = {
                        attr: attr for attr in (year, disturbance_type, age_after, regen_delay)
                        if attr in attribute_table
                    }
                    
                    attributes.update({v: k for k, v in self.transition.items()})
                    disturbance_layer = Layer(
                        layer_path.stem, layer_path, attributes, self.lookup_table_dir,
                        **self.layer_kwargs)

                disturbance_layers.append(DisturbanceLayer(
                    rule_manager,
                    disturbance_layer.to_tiler_layer(rule_manager, **kwargs),
                    Attribute(year) if year in attribute_table else year,
                    Attribute(disturbance_type) if disturbance_type in attribute_table else disturbance_type,
                    transition_rule))
            elif year not in attribute_table:
                kwargs["raw"] = False
                attributes = {
                    attr: attr for attr in (disturbance_type, age_after, regen_delay)
                    if attr in attribute_table
                }

                if spatial_classifier_transition:
                    attributes.update({v: k for k, v in self.transition.items()})

                disturbance_layers.append(DisturbanceLayer(
                    rule_manager,
                    Layer(
                        layer_path.stem, layer_path, attributes,
                        lookup_table=self.lookup_table_dir, **self.layer_kwargs
                    ).to_tiler_layer(rule_manager, **kwargs),
                    year,
                    Attribute(disturbance_type) if disturbance_type in attributes else disturbance_type,
                    transition_rule))
            else:
                # Vector disturbance layers must be split on at least year and
                # possibly disturbance type to avoid overlaps in rasterization.
                kwargs["raw"] = False
                for disturbance_year in attribute_table[year]:
                    attributes = {
                        attr: attr for attr in (year, disturbance_type, age_after, regen_delay)
                        if attr in attribute_table
                    }

                    if spatial_classifier_transition:
                        attributes.update({v: k for k, v in self.transition.items()})

                    attr_filter = {attributes[year]: disturbance_year}

                    year_layer = Layer(
                        f"{layer_path.stem}_{disturbance_year}",
                        str(layer_path), attributes, self.lookup_table_dir, attr_filter,
                        **self.layer_kwargs)

                    disturbance_layers.append(DisturbanceLayer(
                        rule_manager,
                        year_layer.to_tiler_layer(rule_manager, **kwargs),
                        Attribute(year),
                        Attribute(disturbance_type) if disturbance_type in attribute_table else disturbance_type,
                        transition_rule))

        return disturbance_layers

    def _get_disturbance_year_or_attribute(self, layer_path, attribute_table):
        if self.year is not None:
            return self.year

        # Check for the first attribute where all the unique values could be
        # interpreted as a disturbance year.
        for attribute, values in attribute_table.items():
            if all((self._looks_like_disturbance_year(v) for v in values if v is not None)):
                return attribute
        
        # Then check if the disturbance year is parseable from the filename.
        parse_result = re.search(r"(\d{4})", str(layer_path))
        if parse_result is not None:
            return int(parse_result.group())

        raise RuntimeError(f"No disturbance year configured or found in {layer_path}.")

    def _get_disturbance_type_or_attribute(self, layer_path, attribute_table):
        if self.disturbance_type is not None:
            return self.disturbance_type

        gcbm_disturbance_types = self.input_db.get_disturbance_types()
        for attribute, values in attribute_table.items():
            if all((v in gcbm_disturbance_types for v in values)):
                return attribute

        raise RuntimeError(f"No disturbance type configured or found in {layer_path}.")

    def get_configured_or_default(self, attribute_table, attribute, configured_value):
        if configured_value is not None:
            return configured_value

        if attribute in attribute_table:
            return attribute

        return None

    def _looks_like_disturbance_year(self, value):
        # If it parses to an int and has 4 digits, it's probably a year. We don't
        # try full date parsing here because there could be attributes with all
        # kinds of numeric values that aren't disturbance year.
        if len(str(value)) != 4:
            return False

        try:
            int(value)
        except:
            return False

        return True
