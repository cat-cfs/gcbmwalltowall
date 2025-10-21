from __future__ import annotations

import json
import logging
import shutil
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from arrow_space.flattened_coordinate_dataset import create as create_arrowspace_dataset
from arrow_space.input.input_layer_collection import InputLayerCollection
from cbm4.app.spatial.gcbm_input.gcbm_preprocessor_app import preprocess
from cbm_defaults.app import run as make_cbm_defaults
from sqlalchemy import create_engine

from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.converter.layerconverter import (
    DefaultLayerConverter,
    DelegatingLayerConverter,
    LandClassLayerConverter,
)
from gcbmwalltowall.util.path import Path


class ProjectConverter:

    def __init__(self, creation_options=None, disturbance_cohorts=False):
        self._disturbance_cohorts = disturbance_cohorts
        self._creation_options: dict[str, Any] = {
            "chunk_options": {
                "chunk_x_size_max": 2500,
                "chunk_y_size_max": 2500,
            }
        }

        self._creation_options.update(creation_options or {})

    def convert(
        self,
        project,
        output_path,
        aidb_path=None,
        spinup_disturbance_type=None,
        apply_departial_dms=False,
        preserve_temp_files=False,
    ):
        with TemporaryDirectory() as temp_path:
            temp_dir = Path(temp_path)
            output_path = Path(output_path)
            aidb_path = Path(aidb_path) if aidb_path else None
            shutil.rmtree(output_path, ignore_errors=True)
            output_path.mkdir(parents=True, exist_ok=True)

            self._convert_yields(project, temp_dir)
            self._build_input_database(project, temp_dir, aidb_path)
            use_cohorts = self._cohorts_enabled(project)

            transition_disturbed_path = temp_dir.joinpath(
                "transitions.csv" if not use_cohorts else "transition_disturbed.csv"
            )

            transition_rules_disturbed_path = temp_dir.joinpath(
                "transition_rules.csv"
                if not use_cohorts
                else "transition_rules_disturbed.csv"
            )

            transitions = self._get_transitions(project)
            if not transitions.empty:
                transitions.to_csv(transition_disturbed_path, index=False)

            transition_rules = self._get_transition_rules(project)
            if not transition_rules.empty:
                transition_rules.to_csv(transition_rules_disturbed_path, index=False)

            if use_cohorts:
                transition_undisturbed = self._get_transition_undisturbed(project)
                if not transition_undisturbed.empty:
                    transition_undisturbed.to_csv(
                        temp_dir.joinpath("transition_undisturbed.csv"), index=False
                    )

                transition_rules_undisturbed = self._get_transition_rules_undisturbed(project)
                if not transition_rules_undisturbed.empty:
                    transition_rules_undisturbed.to_csv(
                        temp_dir.joinpath("transition_rules_undisturbed.csv"), index=False
                    )

            subconverters = [
                LandClassLayerConverter(),
                DefaultLayerConverter(
                    name_remappings={
                        "initial_age": "age",
                        "inventory_delay": "delay",
                    }
                ),
            ]

            cbm4_config = self._create_cbm4_config(
                project, output_path, spinup_disturbance_type
            )
            layer_converter = DelegatingLayerConverter(subconverters)
            self._convert_spatial_data(layer_converter, project, temp_dir)
            preprocess_config = {
                "data_dir": str(temp_dir),
                "inventory_dataset": {
                    "dataset_name": "inventory",
                    "storage_type": "local_storage",
                    "path_or_uri": str(output_path.joinpath("inventory")),
                },
                "disturbance_dataset": {
                    "dataset_name": "disturbance",
                    "storage_type": "local_storage",
                    "path_or_uri": str(output_path.joinpath("disturbance")),
                },
                "timestep_interpreter": {
                    "type": "year_offset",
                    "year_offset": cbm4_config["start_year"] - 1,
                },
                "disturbance_event_sorter": {
                    "type": "list",
                    "sort_order": cbm4_config["disturbance_order"],
                },
                "area_unit_conversion": 0.0001,  # ha/m^2
                "cbm_defaults_locale": cbm4_config.get("cbm_defaults_locale", "en-CA"),
                "inventory_override_values": cbm4_config.get(
                    "default_inventory_values"
                ),
                "max_workers": self._creation_options.get("max_workers"),
                "apply_departial_dms": apply_departial_dms,
            }

            for extra_data_file in (
                project.disturbance_rules_path,
                project.rule_based_disturbances_path,
            ):
                if extra_data_file.exists():
                    shutil.copyfile(
                        extra_data_file,
                        temp_dir.joinpath(extra_data_file.name)
                    )

            preprocess(preprocess_config)
            if preserve_temp_files:
                shutil.copytree(temp_dir, output_path.joinpath("temp"))

    @contextmanager
    def _input_db_connection(self, project):
        input_db_path = (
            project.rollback_db_path if project.has_rollback else project.input_db_path
        )

        connection_url = f"sqlite:///{input_db_path}"
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            yield conn

    def _cohorts_enabled(self, project):
        use_cohorts = (
            self._disturbance_cohorts
            or project.transition_undisturbed_path.exists()
            or project.transition_rules_undisturbed_path.exists()
            or len(project.cohorts) > 0
        )

        return use_cohorts

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
            "ArchiveIndex_Beta_Install.mdb",
        )

        if default_aidb_path.exists():
            return default_aidb_path

        raise IOError("Failed to locate AIDB.")

    def _convert_spatial_data(self, layer_converter, project, output_path):
        output_path = Path(output_path)
        base_arrowspace_layers = layer_converter.convert(project.layers)
        base_arrowspace_collection = InputLayerCollection(base_arrowspace_layers)

        creation_options = self._creation_options.copy()
        mask_layers = ["age"] + [
            mask
            for mask in project.masks
            if mask in base_arrowspace_collection.layer_names
        ]

        for optional_mask_layer in ["admin_boundary", "eco_boundary"]:
            if optional_mask_layer in base_arrowspace_collection.layer_names:
                mask_layers.append(optional_mask_layer)

        creation_options.update({"mask_layers": mask_layers})

        base_dataset_name = "inventory.arrowspace"
        create_arrowspace_dataset(
            base_arrowspace_collection,
            "inventory",
            "local_storage",
            str(
                output_path.joinpath(
                    base_dataset_name
                    + (".cohort0" if self._cohorts_enabled(project) else "")
                )
            ),
            creation_options,
        )

        for i, cohort in enumerate(project.cohorts, 1):
            dataset_name = base_dataset_name + f".cohort{i}"
            cohort_arrowspace_layers = layer_converter.convert(cohort)
            cohort_layer_names = [l.name for l in cohort_arrowspace_layers]
            for base_layer in base_arrowspace_layers:
                if (
                    "historic_disturbance" in base_layer.tags
                    or "last_pass_disturbance" in base_layer.tags
                    or base_layer.name in cohort_layer_names
                ):
                    continue

                cohort_arrowspace_layers.append(base_layer)

            cohort_arrowspace_collection = InputLayerCollection(
                cohort_arrowspace_layers
            )
            create_arrowspace_dataset(
                cohort_arrowspace_collection,
                "inventory",
                "local_storage",
                str(output_path.joinpath(dataset_name)),
                creation_options,
            )

    def _flatten_pivot_columns(self, pivot_data):
        pivot_data.columns = [
            (
                pivot_data.columns.get_level_values(1)[i]
                if pivot_data.columns.get_level_values(1)[i] != ""
                else pivot_data.columns.get_level_values(0)[i]
            )
            for i in range(len(pivot_data.columns))
        ]

    def _convert_yields(self, project, output_path):
        with self._input_db_connection(project) as conn:
            components = (
                pd.read_sql(
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
                """,
                    conn,
                )
                .pivot(index="growth_curve_component_id", columns="classifier_name")
                .reset_index()
                .set_index("growth_curve_component_id")
            )
            self._flatten_pivot_columns(components)

            component_species = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, s.name AS species
                FROM growth_curve_component gcc
                INNER JOIN species s
                    ON gcc.species_id = s.id
                """,
                conn,
            ).set_index("growth_curve_component_id")

            component_values = pd.read_sql(
                """
                SELECT gcc.id AS growth_curve_component_id, gcv.age, gcv.merchantable_volume
                FROM growth_curve_component gcc
                INNER JOIN growth_curve_component_value gcv
                    ON gcc.id = gcv.growth_curve_component_id
                """,
                conn,
            ).pivot(index="growth_curve_component_id", columns="age")
            self._flatten_pivot_columns(component_values)

            yield_output_path = output_path.joinpath("yield.csv")
            yield_curves = (
                components.join(component_species).join(component_values).reset_index()
            )
            yield_curves.drop("growth_curve_component_id", axis=1).to_csv(
                yield_output_path, index=False
            )

    def _get_transitions(self, project):
        with self._input_db_connection(project) as conn:
            transitions = (
                pd.read_sql(
                    """
                SELECT
                    t.id,
                    t.regen_delay AS "state.regeneration_delay",
                    CASE WHEN t.age = -1 THEN '?' ELSE t.age END AS "state.age",
                    'classifiers.' || c.name AS classifier_name,
                    cv.value AS classifier_value
                FROM transition t
                INNER JOIN transition_classifier_value tcv
                    ON t.id = tcv.transition_id
                INNER JOIN classifier_value cv
                    ON tcv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """,
                    conn,
                )
                .pivot(
                    index=["id", "state.regeneration_delay", "state.age"],
                    columns="classifier_name",
                )
                .reset_index()
            )
            self._flatten_pivot_columns(transitions)

        return self._sort_transition_data_cols(transitions)

    def _get_transition_rules(self, project):
        transitions = self._get_transitions(project)
        with self._input_db_connection(project) as conn:
            transition_rules = (
                pd.read_sql(
                    """
                SELECT
                    tr.id,
                    tr.transition_id,
                    dt.code AS "parameters.disturbance_type_match",
                    'classifiers.' || c.name || '_match' AS classifier_name,
                    cv.value AS classifier_value
                FROM transition_rule tr
                INNER JOIN disturbance_type dt
                    ON tr.disturbance_type_id = dt.id
                INNER JOIN transition_rule_classifier_value tcv
                    ON tr.id = tcv.transition_rule_id
                INNER JOIN classifier_value cv
                    ON tcv.classifier_value_id = cv.id
                INNER JOIN classifier c
                    ON cv.classifier_id = c.id
                """,
                    conn,
                )
                .pivot(
                    index=["id", "transition_id", "parameters.disturbance_type_match"],
                    columns="classifier_name",
                )
                .reset_index()
            )
            self._flatten_pivot_columns(transition_rules)

        transition_rule_data = transition_rules.merge(
            transitions, left_on="transition_id", right_on="id", suffixes=(None, "_")
        )

        transition_rule_data.drop(
            ["transition_id"]
            + [c for c in transition_rule_data.columns if c.endswith("_")],
            axis=1,
            inplace=True,
        )

        return self._sort_transition_data_cols(transition_rule_data)

    def _get_transition_undisturbed(self, project):
        if not project.transition_undisturbed_path.exists():
            return pd.DataFrame()

        return self._format_transition_undisturbed(
            project, pd.read_csv(str(project.transition_undisturbed_path))
        )

    def _get_transition_rules_undisturbed(self, project):
        if not project.transition_rules_undisturbed_path.exists():
            return pd.DataFrame()

        return self._format_transition_undisturbed(
            project, pd.read_csv(str(project.transition_rules_undisturbed_path))
        )

    def _format_transition_undisturbed(self, project, transition_data):
        if "disturbance_type" in transition_data.columns:
            project_dist_types = self._load_disturbance_types(project)
            dist_type_map = pd.DataFrame(
                {
                    "disturbance_type": project_dist_types.keys(),
                    "disturbance_type_id": project_dist_types.values(),
                }
            )

            transition_data = transition_data.merge(
                dist_type_map, on="disturbance_type"
            )
            transition_data.drop("disturbance_type", axis=1, inplace=True)

        transition_data[transition_data.loc[transition_data["age_after"] == -1]] = "?"
        transition_data.rename(
            columns={
                "age_after": "state.age",
                "regen_delay": "state.regeneration_delay",
                "disturbance_type_id": "parameters.disturbance_type_match",
            },
            inplace=True,
        )

        for classifier in project.classifiers:
            transition_data.rename(
                columns={
                    classifier: f"classifiers.{classifier}",
                    f"{classifier}_match": f"classifiers.{classifier}_match",
                },
                inplace=True,
            )

        return self._sort_transition_data_cols(transition_data)

    def _sort_transition_data_cols(self, transition_data):
        cols = transition_data.columns.tolist()
        sorted_cols = sorted(
            cols,
            key=lambda item: (
                0
                if item == "id"
                else (
                    1
                    if "disturbance_type" in item
                    else (
                        2
                        if item.endswith("_match")
                        else (
                            3
                            if item == "state.age"
                            else 4 if item == "state.regeneration_delay" else 5
                        )
                    )
                )
            ),
        )

        return transition_data[sorted_cols]

    def _build_input_database(self, project, output_path, aidb_path=None):
        aidb_path = aidb_path or self._find_aidb_path(project)
        output_cbm_defaults_path = output_path.joinpath("cbm_defaults.db")
        if aidb_path.suffix == ".db":
            shutil.copyfile(aidb_path, output_cbm_defaults_path)
        else:
            make_cbm_defaults(
                {
                    "output_path": output_cbm_defaults_path,
                    "default_locale": "en-CA",
                    "locales": [{"id": 1, "code": "en-CA"}],
                    "archive_index_data": [{"locale": "en-CA", "path": str(aidb_path)}],
                }
            )

        return output_cbm_defaults_path

    def _load_disturbance_order(self, project: PreparedProject) -> dict[str, int]:
        ordered_db_dist_types = self._load_disturbance_types(project)
        # ensure no duplicates in the user disturbance type order
        user_disturbance_order = project.disturbance_order
        unique_user_dist_types = set(user_disturbance_order)
        if not len(unique_user_dist_types) == len(user_disturbance_order):
            raise ValueError(
                f"duplicate values detected in user disturbance type order"
            )

        # check that every disturbance type in the user order exists in the database
        unknown_disturbance_types = unique_user_dist_types.difference(
            set(ordered_db_dist_types.keys())
        )

        if unknown_disturbance_types:
            logging.warn(
                "entries in user disturbance type order not found in database - ignoring: "
                f"{unknown_disturbance_types}"
            )

            for unknown_disturbance_type in unknown_disturbance_types:
                user_disturbance_order.remove(unknown_disturbance_type)

        output_order = [
            ordered_db_dist_types[dist_type] for dist_type in user_disturbance_order
        ] + [
            dist_code
            for dist_type, dist_code in ordered_db_dist_types.items()
            if dist_type not in unique_user_dist_types
        ]

        return output_order

    def _load_disturbance_types(self, project) -> dict:
        with self._input_db_connection(project) as conn:
            dist_types = pd.read_sql_query(
                """
                SELECT code, name
                FROM disturbance_type
                WHERE code > 0
                ORDER BY code
                """,
                conn,
            )

        return {str(row["name"]): int(row["code"]) for _, row in dist_types.iterrows()}

    def _create_cbm4_config(self, project, output_path, spinup_disturbance_type=None):
        default_inventory_values = {}

        cset_config_file = GCBMConfigurer.find_config_file(
            project.gcbm_config_path, "Variables", "initial_classifier_set"
        )

        classifiers = json.load(open(cset_config_file, "rb"))["Variables"][
            "initial_classifier_set"
        ]["transform"]["vars"]

        classifiers.extend(["admin_boundary", "eco_boundary"])

        for classifier in classifiers:
            config_file = GCBMConfigurer.find_config_file(
                project.gcbm_config_path, "Variables", classifier
            )

            classifier_value = json.load(open(config_file, "rb"))["Variables"][
                classifier
            ]
            if isinstance(classifier_value, dict):
                continue

            default_inventory_values[classifier] = classifier_value

        if spinup_disturbance_type:
            default_inventory_values["historic_disturbance_type"] = (
                spinup_disturbance_type
            )
            default_inventory_values["last_pass_disturbance_type"] = (
                spinup_disturbance_type
            )

        if not GCBMConfigurer.find_config_file(
            project.gcbm_config_path, "Variables", "inventory_delay"
        ):
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
            "apply_departial_dms": self._disturbance_cohorts,
            "use_smoother": project.use_smoother,
            "disturbance_order": self._load_disturbance_order(project),
        }

        json.dump(config, open(output_path.joinpath("cbm4_config.json"), "w"), indent=4)

        return config
