import logging
import json
import pandas as pd
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from pathlib import Path
from typing import Any
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.converter.layerconverter import DefaultLayerConverter
from gcbmwalltowall.component.preparedproject import PreparedLayer
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from arrow_space import flattened_coordinate_dataset
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from arrow_space.flattened_coordinate_dataset import InputLayerCollection
from arrow_space.input.flattened_coordinate_input_layer import FlattenedCoordinateInputLayer
from cbm4.app.spatial.gcbm_input.disturbance_event_sorter import ListBasedSorter
from cbm4.app.spatial.gcbm_input.gcbm_disturbance_preprocessor import (
    GCBMDisturbancePreprocessor,
    GCBMInputReader,
)
from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project
from gcbmwalltowall.project.projectfactory import ProjectFactory
from gcbmwalltowall.component.inputdatabase import InputDatabase
from mojadata.cleanup import cleanup
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import SharedTransitionRuleManager
from mojadata.boundingbox import BoundingBox
from mojadata.layer.rasterlayer import RasterLayer
from cbm4.app.spatial.gcbm_input.timestep_interpreter import (
    YearOffsetTimestepInterpreter,
)


@dataclass
class _Classifier:
    name: str


class GCBMDisturbanceInputReader(GCBMInputReader):

    def __init__(
        self,
        gcbm_disturbance_dataset: FlattenedCoordinateDataset,
        cbm4_defaults_path: str,
        cbm4_defaults_locale: str,
    ):
        self._temp_dir = TemporaryDirectory()
        self._gcbm_disturbance_dataset = gcbm_disturbance_dataset
        self._cbm_defaults_path = cbm4_defaults_path
        self._cbm_defaults_locale = cbm4_defaults_locale
        self._input_datasets_by_cohort = {0: gcbm_disturbance_dataset}
        self._disturbance_type_map = self._get_disturbance_map()
        self._disturbance_layer_names = self._get_disturbance_layer_names()

    @property
    def transition_rules_disturbed(self) -> pd.DataFrame | None:
        return self._read_table("transition_rules_disturbed")

    @property
    def transition_rules_undisturbed(self) -> pd.DataFrame | None:
        return self._read_table("transition_rules_undisturbed")

    @property
    def transitions_disturbed(self) -> pd.DataFrame | None:
        return self._read_table("transitions_disturbed")

    @property
    def transitions_undisturbed(self) -> pd.DataFrame | None:
        return self._read_table("transitions_undisturbed")

    @property
    def cohort_filter(self) -> pd.DataFrame | None:
        return self._read_table("cohort_filter")

    @property
    def cohort_sort(self) -> pd.DataFrame | None:
        return self._read_table("cohort_sort")

    @property
    def rule_based_disturbances(self) -> pd.DataFrame | None:
        return self._read_table("events")

    @property
    def disturbance_rules_path(self) -> str:
        disturbance_rules_path = "null"
        if self._gcbm_disturbance_dataset.file_or_dir_exists("disturbance_rules"):
            disturbance_rules_path = str(
                Path(self._temp_dir.name).joinpath("disturbance_rules.json")
            )

            self._gcbm_disturbance_dataset.extract_file_or_dir(
                "disturbance_rules", disturbance_rules_path
            )

        return disturbance_rules_path

    def _read_table(self, table_name: str) -> pd.DataFrame | None:
        if not self._gcbm_disturbance_dataset.table_exists(table_name):
            return None

        return self._gcbm_disturbance_dataset.read_table_pandas(table_name)


class DisturbanceExtender:

    def __init__(self, cbm4_project: CBM4Project, use_cache: bool = True):
        self._cbm4_project = cbm4_project
        self._use_cache = use_cache
        self._temp_dir = TemporaryDirectory()

    def add_from_walltowall_config(
        self,
        disturbance_config_path: str | Path,
    ):
        tiled_output_path = Path(self._temp_dir.name).joinpath("tiled_disturbances")
        self._tile_disturbances(disturbance_config_path, tiled_output_path)
        self.add_from_study_area(Path(tiled_output_path).joinpath("study_area.json"))

    def add_from_study_area(self, study_area_path: str | Path):
        x_chunk_size, y_chunk_size = self._cbm4_project.chunk_size
        addon_disturbance_ds = self._make_walltowall_disturbance_dataset(
            study_area_path,
            Path(self._temp_dir.name).joinpath("addon_disturbances"),
            {
                "chunk_options": {
                    "chunk_x_size_max": x_chunk_size,
                    "chunk_y_size_max": y_chunk_size,
                }
            },
        )

        min_addon_year = pd.concat(
            (
                addon_disturbance_ds.get_attributes(layer)
                for layer in addon_disturbance_ds.get_layer_names()
            )
        )["year"].min()

        base_flattened_disturbance_ds = self._cbm4_project.extract_flattened_disturbances()
        all_flattened_disturbances = self._merge_flattened_disturbances(
            base_flattened_disturbance_ds, addon_disturbance_ds
        )

        gcbm_input_reader = GCBMDisturbanceInputReader(
            all_flattened_disturbances,
            str(self._cbm4_project.cbm_defaults_path),
            self._cbm4_project.cbm_defaults_locale
        )

        preprocessor = GCBMDisturbancePreprocessor(
            YearOffsetTimestepInterpreter(self._cbm4_project.t0_year),
            ListBasedSorter(self._cbm4_project.disturbance_order + [0]),
            gcbm_input_reader,
        )

        out_ds_config = self._cbm4_project.disturbance_dataset_config
        processed_disturbances = preprocessor.create_output_dataset(
            out_ds_config["dataset_name"],
            out_ds_config["storage_type"],
            out_ds_config["path_or_uri"],
        )

        partitions = gcbm_input_reader.get_cohort_partition_values(0)
        for partition_value in partitions:
            preprocessor.process_partition(0, partition_value, processed_disturbances)

        max_disturbance_year = (
            processed_disturbances.read_polars().select("year").max().collect().item()
        )

        with GCBMConfigurer.update_json_file(
            self._cbm4_project.config_path
        ) as cbm4_config:
            cbm4_config["end_year"] = max(cbm4_config["end_year"], max_disturbance_year)
            if self._use_cache:
                cache_config = cbm4_config.get("cache")
                if cache_config:
                    cache_config["end_year"] = min(
                        cache_config["end_year"], min_addon_year - 1
                    )
            else:
                cbm4_config.pop("cache", None)

    def _tile_disturbances(
        self,
        disturbance_config_path: str | Path,
        output_path: str | Path,
    ):
        output_path = Path(output_path)
        disturbance_config = Configuration.load(disturbance_config_path)
        input_db = InputDatabase(self._cbm4_project.cbm_defaults_path, "", None)
        classifiers = [
            _Classifier(l)
            for l in self._cbm4_project.inventory_dataset.get_layer_names()
            if "classifier" in self._cbm4_project.inventory_dataset.get_tags(l)
        ]

        disturbances, rule_based_disturbances = ProjectFactory()._create_disturbances(
            disturbance_config, classifiers, input_db
        )

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()
        with cleanup():
            logging.info("Starting up tiler...")
            bbox_path = str(self._cbm4_project.extract_bounding_box())
            bbox = BoundingBox(RasterLayer(bbox_path), preprocessed=True)
            tiler = GdalTiler2D(bbox, use_bounding_box_resolution=True)
            layers = []
            for disturbance in disturbances:
                logging.info(f"Preparing {disturbance.name or disturbance.pattern}")
                layer = disturbance.to_tiler_layer(rule_manager)
                if isinstance(layer, list):
                    layers.extend(layer)
                else:
                    layers.append(layer)

                logging.info(
                    f"Finished preparing {disturbance.name or disturbance.pattern}"
                )

            tiler.tile(layers, str(output_path))
            rule_manager.write_rules(str(output_path.joinpath("transition_rules.csv")))

        logging.info("Finished tiling")

    def _make_walltowall_disturbance_dataset(
        self,
        study_area_path: str | Path,
        output_path: str | Path,
        creation_options: dict[str, Any] | None = None,
    ) -> FlattenedCoordinateDataset:
        study_area_dir = Path(study_area_path).parent
        study_area = json.load(open(study_area_path))
        layers = [
            PreparedLayer(
                layer["name"], study_area_dir.joinpath(f"{layer['name']}_moja.tiff")
            )
            for layer in study_area["layers"]
            if "disturbance" in layer.get("tags", [])
        ]

        transition_offset = self._cbm4_project.get_max_transition_id()
        converter = DefaultLayerConverter(
            attribute_modifiers={
                "transition": lambda v: v + transition_offset,
                "transition_undisturbed": lambda v: v + transition_offset,
            }
        )

        dataset_input = InputLayerCollection(converter.convert(layers))
        dataset = flattened_coordinate_dataset.create(
            dataset_input,
            "disturbance",
            "local_storage",
            str(output_path),
            creation_options or {},
        )

        # Clean up the attribute table for compatibility with existing CBM4
        # disturbances.
        for layer_name in dataset.get_layer_names():
            attribute_table = dataset.meta.get_attribute_table(layer_name).rename(
                columns={
                    "transition": "disturbed_transition_id",
                    "transition_undisturbed": "undisturbed_transition_id",
                }
            )

            if "proportion" not in attribute_table:
                attribute_table["proportion"] = 1.0

            dataset.meta.write_attribute_table(layer_name, attribute_table)


        for transition_table, transition_fn in (
            ("transitions_disturbed", "transition_rules.csv"),
            ("transitions_undisturbed", "undisturbed_transition_rules.csv"),
        ):
            transitions_path = study_area_dir.joinpath(transition_fn)
            if transitions_path.exists():
                transitions = pd.read_csv(transitions_path)
                transitions["id"] += transition_offset
                col_renames = {
                    "regen_delay": "state.regeneration_delay",
                    "age_after": "state.age",
                }

                for col in transitions.columns:
                    if col not in ("id", "regen_delay", "age_after") and "." not in col:
                        col_renames[col] = f"classifiers.{col}"

                transitions.rename(columns=col_renames, inplace=True)
                dataset.write_table(transition_table, transitions)

        return dataset

    def _merge_flattened_disturbances(
        self, *datasets: FlattenedCoordinateDataset
    ) -> FlattenedCoordinateDataset:
        output_ds = flattened_coordinate_dataset.create(
            InputLayerCollection(
                [
                    FlattenedCoordinateInputLayer(
                        ds,
                        ds.get_layer_names(),
                    )
                    for ds in datasets
                ]
            ),
            "disturbance",
            "local_storage",
            str(Path(self._temp_dir.name).joinpath("merged_extended_disturbances")),
            creation_options={
                "chunk_options": {
                    "chunk_x_size_max": datasets[0].chunks[0].x_size,
                    "chunk_y_size_max": datasets[0].chunks[0].y_size,
                },
            }
        )

        output_ds.meta.write_tags(pd.DataFrame({
            "layer_name": output_ds.get_layer_names(),
            "tag": "disturbance"
        }))

        for transition_table in ("transitions_disturbed", "transitions_undisturbed"):
            transition_data = []
            for ds in datasets:
                if ds.table_exists(transition_table):
                    transition_data.append(ds.read_table_pandas(transition_table))

            if transition_data:
                all_transition_data = pd.concat(transition_data).astype(str)
                for col in all_transition_data.columns:
                    if col.startswith("classifiers.") or col == "state.age":
                        all_transition_data.loc[all_transition_data[col].isna(), col] = "?"

                output_ds.write_table(transition_table, all_transition_data)

        return output_ds
