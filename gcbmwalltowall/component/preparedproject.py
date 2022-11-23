import json
import shutil
from datetime import datetime
from pathlib import Path
from spatial_inventory_rollback.gcbm.merge.gcbm_merge_layer_input import MergeInputLayers
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer

class PreparedProject:

    def __init__(self, path):
        self.path = Path(path).absolute()

    @property
    def tiled_layer_path(self):
        return self.path.joinpath("layers/tiled")

    @property
    def rollback_layer_path(self):
        rollback_layer_path = self.path.joinpath("layers/rollback")
        return rollback_layer_path if rollback_layer_path.exists() else None

    @property
    def input_db_path(self):
        return self.path.joinpath("input_database/gcbm_input.db")

    @property
    def rollback_db_path(self):
        rollback_db_path = self.path.joinpath("input_database/rollback_gcbm_input.db")
        return rollback_db_path if rollback_db_path.exists() else None

    @property
    def gcbm_config_path(self):
        return self.path.joinpath("gcbm_project")

    @property
    def has_rollback(self):
        return self.rollback_layer_path is not None

    @property
    def start_year(self):
        config = json.load(open(self.gcbm_config_path.joinpath("localdomain.json")))
        return datetime.strptime(config["LocalDomain"]["start_date"], "%Y/%m/%d").year

    @property
    def end_year(self):
        config = json.load(open(self.gcbm_config_path.joinpath("localdomain.json")))
        return datetime.strptime(config["LocalDomain"]["end_date"], "%Y/%m/%d").year - 1

    @property
    def all_study_area_layers(self):
        layer_metadata = {}
        for layer_path in (self.tiled_layer_path, self.rollback_layer_path):
            if not layer_path:
                continue

            layer_metadata.update({
                layer["name"]: layer for layer in
                json.load(open(layer_path.joinpath("study_area.json")))["layers"]
            })

        return layer_metadata

    @property
    def configured_layers(self):
        config = json.load(open(self.gcbm_config_path.joinpath("provider_config.json")))
        return {
            layer["name"]: layer for layer in
            config["Providers"]["RasterTiled"]["layers"]
        }

    def prepare_merge(self, working_path, priority):
        if not self.has_rollback:
            transition_rules = self.tiled_layer_path.joinpath("transition_rules.csv")
            
            return MergeInputLayers(
                priority,
                str(self.input_db_path),
                str(self.tiled_layer_path.joinpath("study_area.json")),
                str(transition_rules) if transition_rules.exists() else None,
                self.start_year,
                priority == 0)

        # Merge expects a single study_area.json, so for projects that have been
        # rolled back, need to consolidate the layers and study areas.
        staging_path = Path(working_path).joinpath(self.path.stem)
        staging_path.mkdir()

        staging_study_area = staging_path.joinpath("study_area.json")
        shutil.copyfile(self.rollback_layer_path.joinpath("study_area.json"), staging_study_area)

        all_study_area_layers = self.all_study_area_layers
        with GCBMConfigurer.update_json_file(staging_study_area) as study_area:
            study_area["layers"] = []
            for layer_name, layer in self.configured_layers.items():
                study_area["layers"].append(all_study_area_layers[layer_name])
                for layer_file in (
                    self.gcbm_config_path.joinpath(layer["layer_path"]),
                    self.gcbm_config_path.joinpath(layer["layer_path"]).with_suffix(".json")
                ):
                    shutil.copyfile(layer_file, staging_path.joinpath(layer_file.name))

        transition_rules = self.rollback_layer_path.joinpath("transition_rules.csv")

        return MergeInputLayers(
            priority,
            str(self.rollback_db_path),
            str(staging_study_area),
            str(transition_rules) if transition_rules.exists() else None,
            self.start_year,
            priority == 0)
