import csv
import shutil
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

    def __init__(self, name, bounding_box, classifiers, layers, input_db, output_path,
                 disturbances=None, rollback=None):
        self.name = require_not_null(name)
        self.bounding_box = require_instance_of(bounding_box, BoundingBox)
        self.classifiers = require_instance_of(classifiers, list)
        self.layers = require_instance_of(layers, list)
        self.input_db = require_instance_of(input_db, InputDatabase)
        self.output_path = Path(require_not_null(output_path)).absolute()
        self.disturbances = disturbances
        self.rollback = rollback

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

            if self.disturbances:
                for disturbance in self.disturbances:
                    layer = disturbance.to_tiler_layer(rule_manager)
                    if isinstance(layer, list):
                        tiler_layers.extend(layer)
                    else:
                        tiler_layers.append(layer)

            tiler = GdalTiler2D(tiler_bbox, use_bounding_box_resolution=True)
            tiler.tile(tiler_layers, str(self.tiler_output_path))
            rule_manager.write_rules(str(self.tiler_output_path.joinpath("transition_rules.csv")))

    def create_input_database(self, recliner2gcbm_exe):
        output_path = self.input_db_path.parent
        output_path.mkdir(parents=True, exist_ok=True)
        self.input_db.create(
            recliner2gcbm_exe, self.classifiers, self.input_db_path,
            self.tiler_output_path.joinpath("transition_rules.csv").absolute())

    def run_rollback(self, recliner2gcbm_exe):
        if self.rollback:
            self.rollback.run(self.classifiers, self.tiler_output_path, self.input_db_path)
            self.input_db.create(
                recliner2gcbm_exe, self.classifiers, self.rollback_input_db_path,
                self.rollback_output_path.joinpath("transition_rules.csv").absolute())

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
                    attribute_filter))
        
        disturbances = [
            Disturbance(
                config.resolve(pattern), input_db,
                dist_config.get("year"), dist_config.get("disturbance_type"),
                dist_config.get("age_after"), dist_config.get("regen_delay"),
                {c.name: dist_config[c.name] for c in classifiers if c.name in dist_config},
                config.config_path, **{
                    k: v for k, v in dist_config.items()
                    if k not in {"year", "disturbance_type", "age_after", "regen_delay"}
                    and k not in {c.name for c in classifiers if c.name in dist_config}})
            for pattern, dist_config in config.get("disturbances", {}).items()
        ]

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

            rollback = Rollback(
                age_distribution,
                inventory_year_layer.name if inventory_year_layer else inventory_year,
                rollback_year, rollback_config.get("prioritize_disturbances", False),
                rollback_config.get("single_draw", False),
                rollback_config.get("establishment_disturbance_type", "Wildfire"),
                config.gcbm_disturbance_order_path)

        return cls(project_name, bounding_box, classifiers, layers, input_db,
                   str(config.working_path), disturbances, rollback)

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
