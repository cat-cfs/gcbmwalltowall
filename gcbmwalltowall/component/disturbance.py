import fnmatch
import logging
import re
import sys
from itertools import product

from mojadata.layer.attribute import Attribute
from mojadata.layer.gcbm.disturbancelayer import DisturbanceLayer
from mojadata.layer.gcbm.transitionrule import TransitionRule
from mojadata.util import ogr

from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.component.tileable import Tileable
from gcbmwalltowall.util.path import Path


class Disturbance(Tileable):

    def __init__(
        self,
        pattern,
        input_db,
        year=None,
        disturbance_type=None,
        transition=None,
        transition_undisturbed=None,
        lookup_table=None,
        filters=None,
        split_on=None,
        name=None,
        layers=None,
        metadata_attributes=None,
        proportion=None,
        **layer_kwargs,
    ):
        self.pattern = Path(pattern)
        self.input_db = input_db
        self.year = year
        self.disturbance_type = disturbance_type
        self.transition = transition
        self.transition_undisturbed = transition_undisturbed
        self.lookup_table = Path(lookup_table) if lookup_table else None
        self.filters = filters or {}
        self.split_on = (
            [split_on]
            if isinstance(split_on, str)
            else split_on if split_on else ["year"]
        )
        self.name = name
        self.layers = layers
        self.metadata_attributes = metadata_attributes or []
        self.proportion = proportion
        self.layer_kwargs = layer_kwargs or {}

    def to_tiler_layer(self, rule_manager, **kwargs):
        pattern_root = self.pattern.absolute().parent
        if not pattern_root.exists():
            logging.fatal(
                f"Error scanning for disturbance layer pattern {self.pattern}: "
                f"parent directory {pattern_root} does not exist"
            )

            sys.exit("Fatal error preparing disturbance layers")

        disturbance_layers = []
        for layer_path in pattern_root.glob(self.pattern.name):
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
                    disturbance_layers.extend(
                        self._to_tiler_layer(
                            layer_path, rule_manager, layer_kwargs, **kwargs
                        )
                    )
            else:
                disturbance_layers.extend(
                    self._to_tiler_layer(
                        layer_path, rule_manager, self.layer_kwargs, **kwargs
                    )
                )

        return disturbance_layers

    def _to_tiler_layer(self, layer_path, rule_manager, layer_kwargs, **kwargs):
        disturbance_layers = []
        layer = Layer(
            self._make_tiler_name(layer_path, layer_kwargs.get("layer")),
            layer_path,
            lookup_table=self.lookup_table,
            **layer_kwargs,
        )

        attribute_table = layer.attribute_table

        transition_disturbed, transition_disturbed_attributes = self._make_transition(
            attribute_table, self.transition
        )

        transition_undisturbed, transition_undisturbed_attributes = (
            self._make_transition(attribute_table, self.transition_undisturbed)
        )

        spatial_classifier_transition = set()
        spatial_classifier_transition.update(set(transition_disturbed_attributes or []))
        spatial_classifier_transition.update(set(transition_undisturbed_attributes or []))

        disturbance_type = self._get_disturbance_type_or_attribute(
            layer_path, attribute_table
        )
        year = self._get_disturbance_year_or_attribute(layer_path, attribute_table)
        proportion = self._get_configured_or_default(
            attribute_table, "proportion", self.proportion
        )

        transition_tiler_attributes = []
        if transition_disturbed:
            for attr in (
                transition_disturbed.age_after,
                transition_disturbed.regen_delay
            ):
                if isinstance(attr, Attribute):
                    transition_tiler_attributes.append(attr.name)

        if transition_undisturbed:
            for attr in (
                transition_undisturbed.age_after,
                transition_undisturbed.regen_delay
            ):
                if isinstance(attr, Attribute):
                    transition_tiler_attributes.append(attr.name)

        tiler_attributes = {
            attr: attr
            for attr in (
                [year, disturbance_type, proportion]
                + transition_tiler_attributes
                + self.metadata_attributes
            )
            if attr in attribute_table
        }

        if spatial_classifier_transition:
            # Transition is configured as classifier to layer attribute;
            # use the inverse to rename those attributes to match the
            # classifiers when tiling.
            tiler_attributes.update({v: k for k, v in self.transition.classifiers.items()})

        layer_filters = {}
        for filter_attr, filter_value in self.filters.items():
            layer_filter_attr = (
                filter_attr
                if filter_attr not in ("year", "disturbance_type")
                else (
                    year
                    if (filter_attr == "year" and year in tiler_attributes)
                    else (
                        disturbance_type
                        if (
                            filter_attr == "disturbance_type"
                            and disturbance_type in tiler_attributes
                        )
                        else None
                    )
                )
            )

            if not layer_filter_attr:
                continue

            layer_filters[layer_filter_attr] = self._parse_filter_value(filter_value)
            tiler_attributes[layer_filter_attr] = layer_filter_attr

        if layer.is_raster:
            if "year" in self.filters and year not in layer_filters:
                # Raster layers support simple year filtering - skip layers
                # not in the allowed range.
                if year not in self._parse_filter_value(self.filters["year"]):
                    return disturbance_layers

            disturbance_layer = layer
            if spatial_classifier_transition:
                disturbance_layer = layer.split(
                    self._make_tiler_name(layer_path), tiler_attributes
                )

            disturbance_layers.append(
                DisturbanceLayer(
                    rule_manager,
                    disturbance_layer.to_tiler_layer(rule_manager, **kwargs),
                    Attribute(year) if year in attribute_table else year,
                    (
                        Attribute(disturbance_type)
                        if disturbance_type in attribute_table
                        else disturbance_type
                    ),
                    transition_disturbed,
                    transition_undisturbed=transition_undisturbed,
                    proportion=(
                        Attribute(proportion)
                        if proportion in attribute_table
                        else proportion
                    ),
                )
            )
        else:
            # Vector disturbance layers sometimes need to be split on year and/or
            # disturbance type to handle overlapping polygons in rasterization.
            kwargs["raw"] = False

            split_attributes = []
            if "year" in self.split_on and year in attribute_table:
                split_attributes.append(year)

            if (
                "disturbance_type" in self.split_on
                and disturbance_type in attribute_table
            ):
                split_attributes.append(disturbance_type)

            if not split_attributes:
                disturbance_layers.append(
                    DisturbanceLayer(
                        rule_manager,
                        layer.split(
                            self._make_tiler_name(
                                layer_path, layer_kwargs.get("layer")
                            ),
                            tiler_attributes,
                            layer_filters,
                        ).to_tiler_layer(rule_manager, **kwargs),
                        Attribute(year) if year in tiler_attributes else year,
                        (
                            Attribute(disturbance_type)
                            if disturbance_type in tiler_attributes
                            else disturbance_type
                        ),
                        transition_disturbed,
                        transition_undisturbed=transition_undisturbed,
                        proportion=(
                            Attribute(proportion)
                            if proportion in tiler_attributes
                            else proportion
                        ),
                    )
                )
            else:
                # Split vector into a raster per combination of split attribute values,
                # while also obeying any configured filters.
                logging.info(f"  splitting on: {', '.join(split_attributes)}")
                split_values = {}
                for split_attr in split_attributes:
                    split_attr_values = attribute_table[split_attr]
                    filter_values = layer_filters.get(split_attr)
                    if isinstance(filter_values, list):
                        split_attr_values = list(
                            set(split_attr_values).intersection(
                                set(
                                    (
                                        type(split_attr_values[0])(v)
                                        for v in filter_values
                                    )
                                )
                            )
                        )

                    split_values[split_attr] = split_attr_values

                non_splitting_filters = {
                    k: v for k, v in layer_filters.items() if k not in split_values
                }

                for i, split_target_values in enumerate(
                    product(*split_values.values())
                ):
                    split_layer_filters = dict(
                        zip(split_values.keys(), split_target_values)
                    )
                    split_layer_filters.update(non_splitting_filters)

                    logging.info(f"    split {i}: {split_layer_filters}")
                    split_layer = layer.split(
                        self._make_tiler_name(layer_path, layer_kwargs.get("layer"), i),
                        tiler_attributes,
                        split_layer_filters,
                    )

                    disturbance_layers.append(
                        DisturbanceLayer(
                            rule_manager,
                            split_layer.to_tiler_layer(rule_manager, **kwargs),
                            Attribute(year) if year in tiler_attributes else year,
                            (
                                Attribute(disturbance_type)
                                if disturbance_type in tiler_attributes
                                else disturbance_type
                            ),
                            transition_disturbed,
                            transition_undisturbed=transition_undisturbed,
                            proportion=(
                                Attribute(proportion)
                                if proportion in tiler_attributes
                                else proportion
                            ),
                        )
                    )

        return disturbance_layers

    def _make_transition(self, attribute_table, transition_config):
        if not transition_config:
            return None, None

        spatial_classifier_transition = None
        age_after = self._get_configured_or_default(
            attribute_table, "age_after", transition_config.age_after
        )

        regen_delay = self._get_configured_or_default(
            attribute_table, "regen_delay", transition_config.regen_delay
        )

        if transition_config.classifiers:
            if all(
                (v in attribute_table for v in transition_config.classifiers.values())
            ):
                spatial_classifier_transition = list(
                    transition_config.classifiers.keys()
                )

        transition_rule = TransitionRule(
            Attribute(regen_delay) if regen_delay in attribute_table else regen_delay,
            Attribute(age_after) if age_after in attribute_table else age_after,
            spatial_classifier_transition or transition_config.classifiers,
        )

        return transition_rule, spatial_classifier_transition

    def _make_tiler_name(self, layer_path, *args):
        return (
            "_".join([self.name, *(str(a) for a in args if a is not None)])
            if self.name
            else "_".join([layer_path.stem, *(str(a) for a in args if a is not None)])
        )

    def _parse_filter_value(self, filter_value):
        if (
            isinstance(filter_value, str)
            and filter_value.startswith("(")
            and filter_value.endswith(")")
        ):
            filter_min, filter_max = eval(filter_value)
            return list(range(filter_min, filter_max + 1))

        return filter_value

    def _try_parse_year(self, layer_path):
        parse_result = re.findall(r"(\d{4})", str(layer_path))
        if parse_result is not None:
            try:
                year = int(parse_result[-1])
                logging.info(f"  using value from filename: {year}")
                return year
            except:
                pass

    def _get_disturbance_year_or_attribute(self, layer_path, attribute_table):
        logging.info(f"  checking for disturbance year in {layer_path.name}...")
        if self.year == "filename":
            year = self._try_parse_year(layer_path)
            if year is None:
                raise RuntimeError(f"Year not parseable from filename in {layer_path}.")

            return year

        if self.year is not None:
            logging.info(f"  using configured value: {self.year}")
            return self.year

        # Check for the first attribute where all the unique values could be
        # interpreted as a disturbance year.
        for attribute, values in attribute_table.items():
            if any((v is not None for v in values)) and all(
                (self._looks_like_disturbance_year(v) for v in values if v is not None)
            ):
                logging.info(f"  using attribute: {attribute}")
                return attribute

        # Then check if the disturbance year is parseable from the filename.
        year = self._try_parse_year(layer_path)
        if year is None:
            raise RuntimeError(
                f"No disturbance year configured or found in {layer_path}."
            )

        return year

    def _get_disturbance_type_or_attribute(self, layer_path, attribute_table):
        logging.info(f"  checking for disturbance type in {layer_path.name}...")
        if self.disturbance_type is not None:
            logging.info(f"  using configured value: {self.disturbance_type}")
            return self.disturbance_type

        gcbm_disturbance_types = self.input_db.get_disturbance_types()
        for attribute, values in attribute_table.items():
            if not values:
                continue

            if all((v in gcbm_disturbance_types for v in values)):
                logging.info(f"  using attribute: {attribute}")
                return attribute

        raise RuntimeError(f"No disturbance type configured or found in {layer_path}.")

    def _get_configured_or_default(self, attribute_table, attribute, configured_value):
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
            val = int(value)
            if not (val > 1000 and val < 2500):
                return False
        except:
            return False

        return True
