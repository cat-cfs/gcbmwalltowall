import shutil
import os
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import SharedTransitionRuleManager

class Project:

    def __init__(self, name, bounding_box, classifiers, layers, input_db):
        self.name = name
        self.bounding_box = bounding_box
        self.classifiers = classifiers
        self.layers = layers
        self.input_db = input_db

    def tile(self, output_path):
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path, exist_ok=True)

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()
        with cleanup():
            tiler_bbox = self.bounding_box.to_tiler_layer(mgr)
            tiler_layers = [project_layer.to_tiler_layer(mgr) for project_layer in self.layers]
            tiler = GdalTiler2D(tiler_bbox, use_bounding_box_resolution=True)
            tiler.tile(tiler_layers, output_path)
            rule_manager.write_rules(rf"{output_path}\transition_rules.csv")

    def create_input_database(self, output_path):
        raise NotImplementedError()
