from __future__ import annotations
import os
import shutil
import json
import pandas as pd
from contextlib import contextmanager
from sqlalchemy import create_engine
from pathlib import Path
from arrow_space.input.input_layer_collection import InputLayerCollection
from arrow_space.flattened_coordinate_dataset import create as create_arrowspace_dataset
from cbm_defaults.app import run as make_cbm_defaults
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.converter.layerconverter import DelegatingLayerConverter
from gcbmwalltowall.converter.layerconverter import DefaultLayerConverter
from gcbmwalltowall.converter.layerconverter import LandClassLayerConverter
from gcbmwalltowall.converter.disturbance.mergingdisturbancelayerconverter import MergingDisturbanceLayerConverter
from gcbmwalltowall.converter.disturbance.mergingtransitionconverter import MergingTransitionConverter

class ProjectConverter:
    
    def __init__(self, creation_options=None, merge_disturbance_matrices=False):
        self._creation_options = creation_options or {
            "chunk_options": {
                "chunk_x_size_max": 2500,
                "chunk_y_size_max": 2500,
            }
        }
        
        self._merge_disturbance_matrices = merge_disturbance_matrices

    def convert(self, project, output_path, aidb_path=None):
        output_path = Path(output_path)
        aidb_path = Path(aidb_path) if aidb_path else None
        shutil.rmtree(output_path, ignore_errors=True)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self._convert_yields(project, output_path)
        cbm_defaults_path = self._build_input_database(project, output_path, aidb_path)

        transitions = self._get_transitions(project)
        if not self._merge_disturbance_matrices:
            transitions.to_csv(output_path.joinpath("sit_transitions.csv"), index=False)

        subconverters = [
            LandClassLayerConverter(),
            DefaultLayerConverter({
                "initial_age": "age",
                "mean_annual_temperature": "mean_annual_temp",
                "inventory_delay": "delay"
            }, include_disturbances=not self._merge_disturbance_matrices)
        ]
        
        if self._merge_disturbance_matrices:
            subconverters.extend([
                MergingDisturbanceLayerConverter(
                    cbm_defaults_path, project.start_year, disturbance_order=project.disturbance_order
                ),
                MergingTransitionConverter(
                    cbm_defaults_path, project.start_year, project.classifiers,
                    transitions, output_path, disturbance_order=project.disturbance_order
                )
            ])
        
        layer_converter = DelegatingLayerConverter(subconverters)

        self._convert_spatial_data(layer_converter, project, output_path)
        self._create_cbm4_config(project, output_path)

    @contextmanager
    def _input_db_connection(self, project):
        input_db_path = (
            project.rollback_db_path if project.has_rollback
            else project.input_db_path
        )
        
        connection_url = f"sqlite:///{input_db_path}"
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            yield conn

    def _find_aidb_path(self, project):
        aidb_keys = ["aidb", "AIDBPath"]
        for json_file in project.path.rglob("*.json"):
            json_data = json.load(open(json_file))
            if not isinstance(json_data, dict):
                continue

            for aidb_key in aidb_keys:
                aidb_path = json_data.get(aidb_key)
                if aidb_path:
                    aidb_path = json_file.parent.joinpath(aidb_path).absolute()
                    if aidb_path.exists():
                        return aidb_path
        
        # Last resort: try the default opscale AIDB path.
        default_aidb_path = Path(
            r"C:\Program Files (x86)\Operational-Scale CBM-CFS3\Admin\DBs",
            "ArchiveIndex_Beta_Install.mdb"
        )
        
        if default_aidb_path.exists():
            return default_aidb_path
        
        raise IOError("Failed to locate AIDB.")

    def _convert_spatial_data(self, layer_converter, project, output_path):
        arrowspace_layers = InputLayerCollection(layer_converter.convert(project.layers))
        creation_options = self._creation_options.copy()
        mask_layers = ["age"]
        for optional_mask_layer in ["admin_boundary", "eco_boundary"]:
            if optional_mask_layer in arrowspace_layers.layer_names:
                mask_layers.append(optional_mask_layer)
        creation_options.update({
            "mask_layers": mask_layers
        })
        
        create_arrowspace_dataset(
            arrowspace_layers, "inventory", "local_storage",
            str(output_path.joinpath("inventory.arrowspace")),
            creation_options
        )

    def _flatten_pivot_columns(self, pivot_data):
        pivot_data.columns = [
            pivot_data.columns.get_level_values(1)[i] if pivot_data.columns.get_level_values(1)[i] != ""
            else pivot_data.columns.get_level_values(0)[i]
            for i in range(len(pivot_data.columns))
        ]

    def _convert_yields(self, project, output_path):
        with self._input_db_connection(project) as conn:
            components = pd.read_sql(
                """
                SELECT
                    gcc.id AS growth_curve_component_id, c.name AS classifier_name,
                    cv.value AS classifier_value
                FROM growth_curve_component gcc
                INNER JOIN growth_curve_classifier_value gccv
                    ON gcc.growth_curve_id = gccv.growth_curve_id
                INNER JOIN classifier_value cv
                    ON gccv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """, conn
            ).pivot(
                index="growth_curve_component_id", columns="classifier_name"
            ).reset_index().set_index("growth_curve_component_id")
            self._flatten_pivot_columns(components)

            component_species = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, s.name AS species
                FROM growth_curve_component gcc
                INNER JOIN species s
                    ON gcc.species_id = s.id
                """, conn
            ).set_index("growth_curve_component_id")

            component_values = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, gcv.age, gcv.merchantable_volume
                FROM growth_curve_component gcc
                INNER JOIN growth_curve_component_value gcv
                    ON gcc.id = gcv.growth_curve_component_id
                """, conn
            ).pivot(index="growth_curve_component_id", columns="age")
            self._flatten_pivot_columns(component_values)

            yield_output_path = output_path.joinpath("yield.csv")
            yield_curves = components.join(component_species).join(component_values).reset_index()
            yield_curves.drop("growth_curve_component_id", axis=1).to_csv(yield_output_path, index=False)

    def _get_transitions(self, project):
        with self._input_db_connection(project) as conn:
            transitions = pd.read_sql(
                """
                SELECT
                    t.id, t.regen_delay, t.age AS age_after,
                    c.name AS classifier_name, cv.value AS classifier_value
                FROM transition t
                INNER JOIN transition_classifier_value tcv
                    ON t.id = tcv.transition_id
                INNER JOIN classifier_value cv
                    ON tcv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """, conn
            ).pivot(index=["id", "regen_delay", "age_after"], columns="classifier_name").reset_index()
            self._flatten_pivot_columns(transitions)

            return transitions

    def _build_input_database(self, project, output_path, aidb_path=None):
        aidb_path = aidb_path or self._find_aidb_path(project)
        output_cbm_defaults_path = output_path.joinpath("cbm_defaults.db")
        if aidb_path.suffix == ".db":
            shutil.copyfile(aidb_path, output_cbm_defaults_path)
        else:
            make_cbm_defaults({
                "output_path": output_cbm_defaults_path,
                "default_locale": "en-CA",
                "locales": [{"id": 1, "code": "en-CA"}],
                "archive_index_data": [{"locale": "en-CA", "path": str(aidb_path)}]
            })

        return output_cbm_defaults_path

    def _create_cbm4_config(self, project, output_path):
        default_inventory_values = {}

        cset_config_file = GCBMConfigurer.find_config_file(
            project.gcbm_config_path, "Variables", "initial_classifier_set")
        
        classifiers = (
            json.load(open(cset_config_file, "rb"))
                ["Variables"]["initial_classifier_set"]["transform"]["vars"]
        )

        classifiers.extend(["admin_boundary", "eco_boundary"])

        for classifier in classifiers:
            config_file = GCBMConfigurer.find_config_file(
                project.gcbm_config_path, "Variables", classifier)
        
            classifier_value = json.load(open(config_file, "rb"))["Variables"][classifier]
            if isinstance(classifier_value, dict):
                continue

            default_inventory_values[classifier] = classifier_value

        if not GCBMConfigurer.find_config_file(project.gcbm_config_path, "Variables", "inventory_delay"):
            default_inventory_values["delay"] = 0

        config = {
            "resolution": project.resolution,
            "cbm4_spatial_dataset": {
                name: {
                    "dataset_name": name,
                    "storage_type": "local_storage",
                    "path_or_uri": name,
                }
                for name in ("inventory", "disturbance", "simulation")
            },
            "default_inventory_values": default_inventory_values,
            "start_year": project.start_year,
            "end_year": project.end_year,
            "disturbance_event_sorter": None
        }
        
        json.dump(config, open(output_path.joinpath("cbm4_config.json"), "w"), indent=4)
