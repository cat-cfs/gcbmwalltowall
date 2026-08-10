import logging
import json
import pandas as pd
import numpy as np
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from pathlib import Path
from typing import Any
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.converter.layerconverter import DefaultLayerConverter
from gcbmwalltowall.component.preparedproject import PreparedLayer
from arrow_space import flattened_coordinate_dataset
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from arrow_space.flattened_coordinate_dataset import InputLayerCollection
from cbm4.app.spatial.gcbm_input.disturbance_event_sorter import ListBasedSorter
from cbm4.app.spatial.gcbm_input.gcbm_disturbance_preprocessor import (
    normalize_sort_order,
)
from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project
from gcbmwalltowall.application.command.impl.disturbancereader import DisturbanceReader
from gcbmwalltowall.project.projectfactory import ProjectFactory
from gcbmwalltowall.component.inputdatabase import InputDatabase
from mojadata.cleanup import cleanup
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import SharedTransitionRuleManager
from mojadata.boundingbox import BoundingBox
from mojadata.layer.rasterlayer import RasterLayer


@dataclass
class _Classifier:
    name: str


class DisturbanceExtender:

    def __init__(self, cbm4_project: CBM4Project):
        self._cbm4_project = cbm4_project
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
        walltowall_disturbance_ds = self._make_walltowall_disturbance_dataset(
            study_area_path,
            Path(self._temp_dir.name).joinpath("addon_disturbances"),
            {
                "chunk_options": {
                    "chunk_x_size_max": x_chunk_size,
                    "chunk_y_size_max": y_chunk_size,
                }
            },
        )

        reader = DisturbanceReader.starting_from(
            self._cbm4_project, walltowall_disturbance_ds
        )

        base_disturbance_ds = self._cbm4_project.disturbance_dataset
        for chunk_index, _ in enumerate(walltowall_disturbance_ds.chunks):
            logging.info(f"Processing chunk {chunk_index}")
            filters = [[("chunk_index", "=", chunk_index)]]
            disturbance_data = reader.read_disturbances(filters)
            base_disturbance_data = base_disturbance_ds.read_pandas(filters=filters)
            if base_disturbance_data is not None and not base_disturbance_data.empty:
                if disturbance_data is None or disturbance_data.empty:
                    disturbance_data = base_disturbance_data
                else:
                    disturbance_data = pd.concat(
                        (
                            disturbance_data,
                            base_disturbance_ds.as_flattened(base_disturbance_data),
                        )
                    )

            if disturbance_data is None or disturbance_data.empty:
                logging.info(f"  no disturbance data - skipping")
                continue

            sorter = ListBasedSorter(self._cbm4_project.disturbance_order)
            disturbance_data["sort_value"] = disturbance_data[
                "default_disturbance_type_id"
            ].map(sorter.get_sort_value)

            disturbance_data.sort_values(
                by=["raster_index", "timestep", "sort_value"],
                ignore_index=True,
                inplace=True,
            )

            disturbance_data["disturbance_order"] = normalize_sort_order(
                disturbance_data["raster_index"].to_numpy(),
                disturbance_data["timestep"].to_numpy(),
            )

            disturbance_data.drop(
                ["index", "disturbance_id", "sort_value"], axis=1, inplace=True
            )

            disturbance_data, raster_index_data = base_disturbance_ds.as_raster_indexed(
                disturbance_data
            )

            disturbance_data["disturbance_id"] = np.arange(
                start=1, stop=len(disturbance_data) + 1
            )

            base_disturbance_ds.write(disturbance_data)
            base_disturbance_ds.write(
                raster_index_data, base_disturbance_ds.raster_index_table_name
            )

        for table_name, transition_data in reader.read_transitions().items():
            all_transition_data = (
                pd.concat(
                    (base_disturbance_ds.read_table_pandas(table_name), transition_data)
                )
                if base_disturbance_ds.table_exists(table_name)
                else transition_data
            ).astype(
                {"id": "str", "state.age": "str", "state.regeneration_delay": "str"}
            )

            for col in all_transition_data.columns:
                if col.startswith("classifiers.") or col == "state.age":
                    all_transition_data.loc[all_transition_data[col].isna(), col] = "?"

            base_disturbance_ds.write_table(table_name, all_transition_data)

    def _tile_disturbances(
        self, disturbance_config_path: str | Path, output_path: str | Path
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
            bbox_path = self._cbm4_project.extract_bounding_box()
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

        converter = DefaultLayerConverter()
        dataset_input = InputLayerCollection(converter.convert(layers))
        dataset = flattened_coordinate_dataset.create(
            dataset_input,
            "disturbance",
            "local_storage",
            str(output_path),
            creation_options or {},
        )

        for transition_table, transition_fn in (
            ("transitions_disturbed", "transition_rules.csv"),
            ("transitions_undisturbed", "undisturbed_transition_rules.csv"),
        ):
            transitions_path = study_area_dir.joinpath(transition_fn)
            if transitions_path.exists():
                transitions = pd.read_csv(transitions_path)
                col_renames = {
                    "regen_delay": "state.regeneration_delay",
                    "age_after": "state.age",
                }

                for col in transitions.columns:
                    if col not in ("id", "regen_delay", "age_after"):
                        col_renames[col] = f"classifiers.{col}"

                transitions.rename(columns=col_renames, inplace=True)
                dataset.write_table(transition_table, transitions)

        return dataset
