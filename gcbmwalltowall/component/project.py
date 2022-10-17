import shutil
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

    def tile(self):
        tiler_output_path = self.output_path.joinpath("layers", "tiled")
        shutil.rmtree(str(tiler_output_path), ignore_errors=True)
        tiler_output_path.mkdir(parents=True, exist_ok=True)

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
            tiler.tile(tiler_layers, str(tiler_output_path))
            rule_manager.write_rules(str(tiler_output_path.joinpath("transition_rules.csv")))

    def create_input_database(self, recliner2gcbm_exe):
        output_path = self.output_path.joinpath("input_database")
        output_path.mkdir(parents=True, exist_ok=True)
        self.input_db.create(recliner2gcbm_exe, self.classifiers, output_path)

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

            bounding_box_layer = Layer(
                "bounding_box", bbox_path, bounding_box_config.get("attribute"),
                config.resolve(bounding_box_lookup_table) if bounding_box_lookup_table else None)

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

            layer = Layer(
                classifier_name, layer_path, classifier_details.get("attribute"),
                config.resolve(layer_lookup_table) if layer_lookup_table else None)
            
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

                layers.append(Layer(
                    layer_name,
                    layer_path,
                    layer_details.get("attribute"),
                    config.resolve(layer_lookup_table) if layer_lookup_table else None))

        disturbances = [
            Disturbance(
                config.resolve(pattern), input_db, dist_config.get("year"),
                dist_config.get("disturbance_type"), dist_config.get("age_after"),
                dist_config.get("regen_delay"), config.working_path.absolute())
            for pattern, dist_config in config.get("disturbances", {}).items()
        ]

        return cls(project_name, bounding_box, classifiers, layers, input_db,
                   str(config.working_path.absolute()), disturbances)
