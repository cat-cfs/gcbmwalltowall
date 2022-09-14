import shutil
import os
from pathlib import Path
from itertools import chain
from mojadata.cleanup import cleanup
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import SharedTransitionRuleManager
from gcbmwalltowall.component.boundingbox import BoundingBox
from gcbmwalltowall.component.classifier import Classifier
from gcbmwalltowall.component.inputdatabase import InputDatabase
from gcbmwalltowall.component.rollback import Rollback
from gcbmwalltowall.component.layer import Layer
from gcbmwalltowall.validation.string import require_not_null
from gcbmwalltowall.validation.generic import require_instance_of

class Project:

    def __init__(self, name, bounding_box, classifiers, layers, input_db, output_path, rollback=None):
        self.name = require_not_null(name)
        self.bounding_box = require_instance_of(bounding_box, BoundingBox)
        self.classifiers = require_instance_of(classifiers, list)
        self.layers = require_instance_of(layers, list)
        self.input_db = require_instance_of(input_db, InputDatabase)
        self.output_path = Path(require_not_null(output_path)).resolve()
        self.rollback = rollback

    def tile(self):
        tiler_output_path = self.output_path.joinpath("layers", "tiled")
        shutil.rmtree(str(tiler_output_path), ignore_errors=True)
        tiler_output_path.mkdir(parents=True, exist_ok=True)

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()
        with cleanup():
            tiler_bbox = self.bounding_box.to_tiler_layer(mgr)
            tiler_layers = [
                project_layer.to_tiler_layer(mgr)
                for project_layer in chain(self.layers, self.classifiers)
            ]

            tiler = GdalTiler2D(tiler_bbox, use_bounding_box_resolution=True)
            tiler.tile(tiler_layers, str(tiler_output_path))
            rule_manager.write_rules(str(tiler_output_path.joinpath("transition_rules.csv")))

    def create_input_database(self):
        raise NotImplementedError()

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
            bounding_box_layer = Layer("bounding_box", config.resolve(bounding_box_config))
        else:
            bounding_box_lookup_table = bounding_box_config.get("lookup_table")
            bounding_box_layer = Layer(
                "bounding_box",
                config.resolve(require_not_null(bounding_box_config.get("layer"))),
                bounding_box_config.get("attribute"),
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
            layer_lookup_table = classifier_details.get("lookup_table")
            layer = Layer(
                classifier_name,
                config.resolve(require_not_null(classifier_details.get("layer"))),
                classifier_details.get("attribute"),
                config.resolve(layer_lookup_table) if layer_lookup_table else None)
            
            classifiers.append(Classifier(
                layer,
                classifier_details.get("values_path"),
                classifier_details.get("values_col"),
                classifier_details.get("yield_col")))

        layers = []
        for layer_name, layer_details in config.get("layers", {}).items():
            if isinstance(layer_details, str):
                layers.append(Layer(layer_name, layer_details))
            else:
                layer_lookup_table = layer_details.get("lookup_table")
                layers.append(Layer(
                    layer_name,
                    config.resolve(require_not_null(layer_details.get("layer"))),
                    layer_details.get("attribute"),
                    config.resolve(layer_lookup_table) if layer_lookup_table else None))

        return Project(project_name, bounding_box, classifiers, layers, input_db,
                       str(config.working_path.resolve()))
