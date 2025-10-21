import json
import shutil
from contextlib import contextmanager
from datetime import datetime

from spatial_inventory_rollback.gcbm.merge.gcbm_merge_layer_input import \
    MergeInputLayers

from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.util.path import Path


class PreparedLayer:

    def __init__(self, name, path):
        self.name = name
        self.path = Path(path)

    @property
    def tiler_metadata(self):
        return json.load(open(self.path.with_suffix(".json"), "rb"))

    @property
    def study_area_metadata(self):
        study_area_metadata = {"name": self.name, "type": "RasterLayer"}

        study_area_path = self.path.parent.joinpath("study_area.json")
        if not study_area_path.exists():
            return study_area_metadata

        study_area_layers = json.load(open(study_area_path))["layers"]
        study_area_metadata.update(
            next((l for l in study_area_layers if l["name"] == self.name))
        )

        return study_area_metadata

    @property
    def metadata(self):
        metadata = self.study_area_metadata
        metadata.update(self.tiler_metadata)

        return metadata


class PreparedProject:

    def __init__(self, path):
        self.path = Path(path).absolute()

    @property
    def resolution(self):
        study_area = json.load(open(self.tiled_layer_path.joinpath("study_area.json")))
        return study_area["pixel_size"]

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
    def cohorts(self):
        cohorts = []
        cohort_tiled_layer_path = self.tiled_layer_path.joinpath("cohorts")
        if cohort_tiled_layer_path.exists():
            for cohort in cohort_tiled_layer_path.iterdir():
                if not cohort.is_dir():
                    continue

                cohort_id = cohort.name
                cohort_layers = []

                cohort_rollback = self.rollback_layer_path.joinpath(
                    "cohorts", cohort_id
                )
                if cohort_rollback.exists():
                    cohort_layers.extend(
                        [
                            PreparedLayer(fn.stem.split("_moja")[0], str(fn.absolute()))
                            for fn in cohort_rollback.glob("*.tiff")
                        ]
                    )

                cohort_layer_names = [l.name for l in cohort_layers]
                cohort_layers.extend(
                    [
                        PreparedLayer(fn.stem.split("_moja")[0], str(fn.absolute()))
                        for fn in cohort.glob("*.tiff")
                        if fn.stem.split("_moja")[0] not in cohort_layer_names
                    ]
                )

                cohorts.append(cohort_layers)

        return cohorts

    @property
    def layers(self):
        config = json.load(open(self.gcbm_config_path.joinpath("provider_config.json")))
        provider_layers = config["Providers"]["RasterTiled"]["layers"]

        return [
            PreparedLayer(
                l["name"], self.gcbm_config_path.joinpath(l["layer_path"]).absolute()
            )
            for l in provider_layers
        ]

    @property
    def disturbance_order(self):
        config = json.load(open(self.gcbm_config_path.joinpath("variables.json"), "rb"))
        return list(
            dict.fromkeys(config["Variables"].get("user_disturbance_order", []))
        )

    @property
    def classifiers(self):
        config = json.load(open(self.gcbm_config_path.joinpath("variables.json"), "rb"))
        return config["Variables"]["initial_classifier_set"]["transform"]["vars"]

    @property
    def use_smoother(self):
        config = json.load(
            open(self.gcbm_config_path.joinpath("modules_cbm.json"), "rb")
        )
        return (
            config["Modules"]["CBMGrowthModule"]
            .get("settings", {})
            .get("smoother_enabled", True)
        )

    @property
    def transition_undisturbed_path(self):
        if self.has_rollback:
            return self.input_db_path.parent.joinpath(
                "gcbmwalltowall_rollback_transition_undisturbed.csv"
            )

        return self.input_db_path.parent.joinpath(
            "gcbmwalltowall_transition_undisturbed.csv"
        )

    @property
    def transition_rules_undisturbed_path(self):
        return self.input_db_path.parent.joinpath(
            "gcbmwalltowall_transition_rules_undisturbed.csv"
        )

    @property
    def masks(self):
        config = json.load(
            open(self.gcbm_config_path.joinpath("modules_cbm.json"), "rb")
        )
        return (
            config["Modules"]["CBMBuildLandUnitModule"]
            .get("settings", {})
            .get("mask_vars", [])
        )

    @property
    def disturbance_rules_path(self):
        return self.input_db_path.parent.joinpath(
            "disturbance_rules.json"
        )

    @property
    def rule_based_disturbances_path(self):
        return self.input_db_path.parent.joinpath(
            "rule_based_disturbances.csv"
        )

    @contextmanager
    def temporary_new_end_year(self, end_year=None):
        if end_year is None:
            try:
                yield
            finally:
                pass
        else:
            localdomain_path = self.gcbm_config_path.joinpath("localdomain.json")
            try:
                with GCBMConfigurer.update_json_file(
                    localdomain_path
                ) as project_config:
                    original_end_date = project_config["LocalDomain"]["end_date"]
                    project_config["LocalDomain"]["end_date"] = f"{end_year + 1}/01/01"

                yield
            finally:
                with GCBMConfigurer.update_json_file(
                    localdomain_path
                ) as project_config:
                    project_config["LocalDomain"]["end_date"] = original_end_date

    def prepare_merge(self, working_path, priority):
        if not self.has_rollback:
            transition_rules = self.tiled_layer_path.joinpath("transition_rules.csv")

            return MergeInputLayers(
                priority,
                str(self.input_db_path),
                str(self.tiled_layer_path.joinpath("study_area.json")),
                str(transition_rules) if transition_rules.exists() else None,
                self.start_year,
                priority == 0,
            )

        # Merge expects a single study_area.json, so for projects that have been
        # rolled back, need to consolidate the layers and study areas.
        staging_path = Path(working_path).joinpath(self.path.stem)
        staging_path.mkdir()

        staging_study_area = staging_path.joinpath("study_area.json")
        shutil.copyfile(
            self.rollback_layer_path.joinpath("study_area.json"), staging_study_area
        )

        with GCBMConfigurer.update_json_file(staging_study_area) as study_area:
            study_area["layers"] = []
            for layer in self.layers:
                study_area["layers"].append(layer.study_area_metadata)
                for layer_file in (layer.path, layer.path.with_suffix(".json")):
                    shutil.copyfile(layer_file, staging_path.joinpath(layer_file.name))

        transition_rules = self.rollback_layer_path.joinpath("transition_rules.csv")

        return MergeInputLayers(
            priority,
            str(self.rollback_db_path),
            str(staging_study_area),
            str(transition_rules) if transition_rules.exists() else None,
            self.start_year,
            priority == 0,
        )
