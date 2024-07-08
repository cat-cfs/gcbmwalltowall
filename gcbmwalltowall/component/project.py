import logging
import csv
import shutil
from uuid import uuid4
from multiprocessing import cpu_count
from datetime import date
from pathlib import Path
from itertools import chain
from mojadata.util import gdal
from mojadata.cleanup import cleanup
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import SharedTransitionRuleManager
from gcbmwalltowall.component.boundingbox import BoundingBox
from gcbmwalltowall.component.classifier import Classifier
from gcbmwalltowall.component.disturbance import Disturbance
from gcbmwalltowall.component.inputdatabase import InputDatabase
from gcbmwalltowall.component.rollback import Rollback
from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.validation.string import require_not_null
from gcbmwalltowall.validation.generic import require_instance_of

class Project:

    _layer_reserved_keywords = {"layer", "lookup_table", "attribute"}
    
    _disturbance_reserved_keywords = {
        "year", "disturbance_type", "age_after", "regen_delay", "lookup_table",
        "pattern"
    }

    def __init__(self, name, bounding_box, classifiers, layers, input_db, output_path,
                 disturbances=None, rollback=None, soft_transition_rules_path=None):
        self.name = require_not_null(name)
        self.bounding_box = require_instance_of(bounding_box, BoundingBox)
        self.classifiers = require_instance_of(classifiers, list)
        self.layers = require_instance_of(layers, list)
        self.input_db = require_instance_of(input_db, InputDatabase)
        self.output_path = Path(require_not_null(output_path)).absolute()
        self.disturbances = disturbances
        self.rollback = rollback
        self.soft_transition_rules_path = (
            Path(soft_transition_rules_path).absolute() if soft_transition_rules_path
            else None
        )

    @property
    def tiler_output_path(self):
        return self.output_path.joinpath("layers", "tiled")

    @property
    def rollback_output_path(self):
        return self.output_path.joinpath("layers", "rollback")

    @property
    def input_db_path(self):
        return self.output_path.joinpath("input_database", "gcbm_input.db")

    @property
    def rollback_input_db_path(self):
        return self.output_path.joinpath("input_database", "rollback_gcbm_input.db")

    def tile(self):
        shutil.rmtree(str(self.tiler_output_path), ignore_errors=True)
        self.tiler_output_path.mkdir(parents=True, exist_ok=True)

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()
        with cleanup():
            logging.info(f"Preparing non-disturbance layers")
            tiler_bbox = self.bounding_box.to_tiler_layer(rule_manager)
            tiler_layers = [
                layer.to_tiler_layer(
                    rule_manager,
                    # For spatial rollback compatibility:
                    data_type=gdal.GDT_Int16
                        if getattr(layer, "name", "") == "initial_age"
                        else None)
                for layer in chain(self.layers, self.classifiers)
            ]

            logging.info(f"Finished preparing non-disturbance layers")
            if self.disturbances:
                for disturbance in self.disturbances:
                    logging.info(f"Preparing {disturbance.name or disturbance.pattern}")
                    layer = disturbance.to_tiler_layer(rule_manager)
                    if isinstance(layer, list):
                        tiler_layers.extend(layer)
                    else:
                        tiler_layers.append(layer)

                    logging.info(f"Finished preparing {disturbance.name or disturbance.pattern}")

            logging.info("Starting up tiler...")
            tiler = GdalTiler2D(tiler_bbox, use_bounding_box_resolution=True, workers=cpu_count())
            tiler.tile(tiler_layers, str(self.tiler_output_path))
            rule_manager.write_rules(str(self.tiler_output_path.joinpath("transition_rules.csv")))

    def create_input_database(self):
        output_path = self.input_db_path.parent
        output_path.mkdir(parents=True, exist_ok=True)
        prepared_transition_rules_path = output_path.joinpath("gcbmwalltowall_transition_rules.csv")
        tiler_transition_rules_path = self.tiler_output_path.joinpath("transition_rules.csv").absolute()
        self._prepare_transition_rules(tiler_transition_rules_path, prepared_transition_rules_path)
        self.input_db.create(self.classifiers, self.input_db_path, prepared_transition_rules_path)

    def run_rollback(self):
        if self.rollback:
            self.rollback.run(self.classifiers, self.tiler_output_path, self.input_db_path)
            output_path = self.input_db_path.parent
            prepared_transition_rules_path = output_path.joinpath("gcbmwalltowall_rollback_transition_rules.csv")
            tiler_transition_rules_path = self.rollback_output_path.joinpath("transition_rules.csv").absolute()
            self._prepare_transition_rules(tiler_transition_rules_path, prepared_transition_rules_path)
            self.input_db.create(self.classifiers, self.rollback_input_db_path, prepared_transition_rules_path)

    def configure_gcbm(self, template_path, disturbance_order=None,
                       start_year=1990, end_year=date.today().year):
        exclusions_file = next(self.rollback_output_path.glob("exclusions.txt"), None)
        excluded_layers = (
            [line[0] for line in csv.reader(open(exclusions_file))]
            if exclusions_file else None)

        input_db_path = self.rollback_input_db_path if exclusions_file else self.input_db_path

        layer_paths = [str(self.tiler_output_path)]
        if exclusions_file:
            layer_paths.append(str(self.rollback_output_path))

        configurer = GCBMConfigurer(
            layer_paths, template_path, input_db_path,
            self.output_path.joinpath("gcbm_project"), start_year, end_year,
            disturbance_order, excluded_layers)
    
        configurer.configure()

    @classmethod
    def from_configuration(cls, config):
        project_name = require_not_null(config.get("project_name"))
        
        if not config.get("bounding_box") and not config.get("layers"):
            raise RuntimeError(
                "Project requires a bounding_box entry or at least one item in "
                "the layers section")

        bounding_box_config = (
            config.get("bounding_box")
            or config.get("layers", {}).get("initial_age")
            or next(iter(config.get("layers", {}).values())))

        if isinstance(bounding_box_config, str):
            bbox_path = config.resolve(bounding_box_config)
            bounding_box_layer = Layer(
                "bounding_box", bbox_path,
                lookup_table=config.find_lookup_table(bbox_path))
        else:
            bbox_path = config.resolve(require_not_null(bounding_box_config.get("layer")))
            bounding_box_lookup_table = (
                bounding_box_config.get("lookup_table")
                or config.find_lookup_table(bbox_path))

            attribute, attribute_filter = Project._extract_attribute(bounding_box_config)

            bounding_box_layer = Layer(
                "bounding_box", bbox_path, attribute,
                config.resolve(bounding_box_lookup_table) if bounding_box_lookup_table else None,
                attribute_filter)

        resolution = config.get("resolution")
        bounding_box = BoundingBox(bounding_box_layer, resolution)

        input_db = InputDatabase(
            config.resolve(require_not_null(config.get("aidb"))),
            config.resolve(require_not_null(config.get("yield_table"))),
            require_instance_of(config.get("yield_interval"), int))

        classifiers = []
        classifier_config = require_instance_of(config.get("classifiers"), dict)
        for classifier_name, classifier_details in classifier_config.items():
            layer_path = config.resolve(require_not_null(classifier_details.get("layer")))
            layer_lookup_table = (
                classifier_details.get("lookup_table")
                or config.find_lookup_table(layer_path))

            attribute, attribute_filter = Project._extract_attribute(classifier_details)

            layer = Layer(
                classifier_name, layer_path, attribute,
                config.resolve(layer_lookup_table) if layer_lookup_table else None,
                attribute_filter)
            
            classifiers.append(Classifier(
                layer,
                config.resolve(classifier_details.get("values_path", config["yield_table"])),
                classifier_details.get("values_col"),
                classifier_details.get("yield_col")))

        layers = []
        for layer_name, layer_details in config.get("layers", {}).items():
            if isinstance(layer_details, str):
                layer_path = config.resolve(layer_details)
                layers.append(Layer(
                    layer_name, layer_path,
                    lookup_table=config.find_lookup_table(layer_path)))
            else:
                layer_path = config.resolve(require_not_null(layer_details.get("layer")))
                layer_lookup_table = (
                    layer_details.get("lookup_table")
                    or config.find_lookup_table(layer_path))

                attribute, attribute_filter = Project._extract_attribute(layer_details)

                layers.append(Layer(
                    layer_name, layer_path, attribute,
                    config.resolve(layer_lookup_table) if layer_lookup_table else None,
                    attribute_filter, **{
                        k: v for k, v in layer_details.items()
                        if k not in Project._layer_reserved_keywords
                    }))
        
        disturbances = []
        for pattern_or_name, dist_config in config.get("disturbances", {}).items():
            if isinstance(dist_config, str):
                disturbance_pattern = dist_config
                disturbances.append(Disturbance(
                    config.resolve(disturbance_pattern), input_db, name=pattern_or_name))
            else:
                disturbances.append(Disturbance(
                    config.resolve(dist_config.get("pattern", pattern_or_name)), input_db,
                    dist_config.get("year"), dist_config.get("disturbance_type"),
                    dist_config.get("age_after"), dist_config.get("regen_delay"),
                    {c.name: dist_config[c.name] for c in classifiers if c.name in dist_config},
                    config.resolve(dist_config.get("lookup_table", config.config_path)),
                    name=pattern_or_name if "pattern" in dist_config else None, **{
                        k: v for k, v in dist_config.items()
                        if k not in Project._disturbance_reserved_keywords
                        and k not in {c.name for c in classifiers if c.name in dist_config}}))

        rollback = None
        rollback_config = config.get("rollback")
        if rollback_config:
            age_distribution = config.resolve(require_not_null(rollback_config.get("age_distribution")))
            rollback_year = rollback_config.get("rollback_year", 1990)

            inventory_year = rollback_config.get("inventory_year")
            inventory_year_layer = None
            if isinstance(inventory_year, str):
                layer_path = config.resolve(inventory_year)
                inventory_year_layer = Layer(
                    "inventory_year", layer_path,
                    lookup_table=config.find_lookup_table(layer_path))
            elif isinstance(inventory_year, dict):
                layer_path = config.resolve(require_not_null(inventory_year.get("layer")))
                layer_lookup_table = (
                    inventory_year.get("lookup_table")
                    or config.find_lookup_table(layer_path))

                inventory_year_layer = Layer(
                    "inventory_year",
                    layer_path,
                    inventory_year.get("attribute"),
                    config.resolve(layer_lookup_table) if layer_lookup_table else None)

            if inventory_year_layer:
                layers.append(inventory_year_layer)

            establishment_disturbance_type = rollback_config.get(
                "establishment_disturbance_type", "Wildfire")

            if config.resolve(establishment_disturbance_type).exists():
                establishment_disturbance_type = config.resolve(establishment_disturbance_type)

            rollback = Rollback(
                age_distribution,
                inventory_year_layer.name if inventory_year_layer else inventory_year,
                rollback_year, rollback_config.get("prioritize_disturbances", False),
                rollback_config.get("single_draw", False),
                establishment_disturbance_type,
                config.gcbm_disturbance_order_path)

        soft_transitions = config.get("transition_rules")
        if soft_transitions:
            soft_transitions = config.resolve(soft_transitions)

        return cls(project_name, bounding_box, classifiers, layers, input_db,
                   str(config.working_path), disturbances, rollback, soft_transitions)

    def _prepare_transition_rules(self, tiler_transition_rules_path, output_path):
        output_path.unlink(True)
        if not (tiler_transition_rules_path.exists() or self.soft_transition_rules_path):
            return None

        all_transition_rules = []
        for transition_path in (tiler_transition_rules_path, self.soft_transition_rules_path):
            if transition_path and transition_path.exists():
                all_transition_rules.extend((
                    row for row in csv.DictReader(open(transition_path, newline=""))))

        for transition in all_transition_rules:
            transition["id"] = transition.get("id", str(uuid4()))
            transition["disturbance_type"] = transition.get("disturbance_type", "")
            transition["age_reset_type"] = transition.get("age_reset_type", "absolute")
            transition["regen_delay"] = transition.get("regen_delay", 0)
            for classifier in self.classifiers:
                transition[classifier.name] = transition.get(classifier.name, "?")
                transition[f"{classifier.name}_match"] = transition.get(f"{classifier.name}_match", "")

        with open(output_path, "w", newline="") as merged_transition_rules:
            header = all_transition_rules[0].keys()
            writer = csv.DictWriter(merged_transition_rules, fieldnames=header)
            writer.writeheader()
            writer.writerows(all_transition_rules)
        
    @staticmethod
    def _extract_attribute(config):
        attribute = config.get("attribute")
        if not attribute:
            return None, None

        attribute_filter = None
        if isinstance(attribute, dict):
            attribute, filter_value = next(iter(attribute.items()))
            attribute_filter = {attribute: filter_value}

        return attribute, attribute_filter
