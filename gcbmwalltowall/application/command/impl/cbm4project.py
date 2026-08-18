import pandas as pd
from tempfile import TemporaryDirectory
from typing import Any
from pathlib import Path
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from arrow_space.operations.export.geotiff_export import GeoTiffExporter
from arrow_space.operations.dataset_conversion.raster_indexed_dataset_conversion import to_single_layer_flattened_dataset
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from arrow_space.flattened_coordinate_dataset import InputLayerCollection
from arrow_space.input.flattened_coordinate_input_layer import FlattenedCoordinateInputLayer
from arrow_space import flattened_coordinate_dataset
from gcbmwalltowall.configuration.configuration import Configuration


class CBM4Project:

    def __init__(self, cbm4_config_path: str | Path):
        self._temp_dir = TemporaryDirectory()
        self._cbm4_config_path = Path(cbm4_config_path)
        self._cbm4_config = Configuration.load(cbm4_config_path)
        self._inventory_dataset = self._get_dataset("inventory")
        self._disturbance_dataset = self._get_dataset("disturbance")
        self._bbox_path = None
        self._cbm_defaults_path = Path(self._temp_dir.name).joinpath("cbm_defaults.db")
        self._inventory_dataset.extract_file_or_dir(
            "cbm_defaults", str(self._cbm_defaults_path)
        )

    @property
    def config_path(self) -> Path:
        return self._cbm4_config_path

    @property
    def inventory_dataset(self) -> RasterIndexedDataset:
        return self._inventory_dataset

    @property
    def disturbance_dataset_config(self) -> dict[str, Any]:
        return self._get_dataset_config("disturbance")

    @property
    def disturbance_dataset(self) -> RasterIndexedDataset:
        return self._disturbance_dataset

    @property
    def t0_year(self) -> int:
        return self._cbm4_config["start_year"] - 1

    @property
    def cbm_defaults_path(self) -> Path:
        return self._cbm_defaults_path

    @property
    def cbm_defaults_locale(self) -> str:
        return self._cbm4_config["cbm_defaults_locale"]

    @property
    def chunk_size(self) -> tuple[int, int]:
        return (
            self._inventory_dataset.chunks[0].x_size,
            self._inventory_dataset.chunks[0].y_size,
        )

    @property
    def disturbance_order(self) -> list[int]:
        return self._cbm4_config["disturbance_order"]

    def get_max_transition_id(self) -> int:
        max_transition_id = 0
        for table_name in ("transitions_disturbed", "transitions_undisturbed"):
            if self._disturbance_dataset.table_exists(table_name):
                max_transition_id = max(
                    max_transition_id,
                    self._disturbance_dataset.read_table_pandas(table_name)["id"]
                    .astype("int")
                    .max(),
                )

        return max_transition_id

    def extract_bounding_box(self) -> Path:
        if self._bbox_path is not None:
            return self._bbox_path

        inv_data = self._inventory_dataset.read_polars()
        inv_ri_data = self._inventory_dataset.read_polars(
            self._inventory_dataset.raster_index_table_name
        )

        export_data = (
            inv_data.join(inv_ri_data, on=["chunk_index", "index", "cohort_index"])
            .select("chunk_index", "raster_index")
            .unique()
            .with_columns(bbox=1)
            .collect()
            .to_pandas()
        )

        exporter = GeoTiffExporter(self._inventory_dataset)
        exporter.write_data(export_data)
        exporter.write_geotiff(self._temp_dir.name, "GTiff")
        self._bbox_path = Path(self._temp_dir.name).joinpath("bbox.tiff")

        return self._bbox_path

    def extract_flattened_disturbances(self) -> FlattenedCoordinateDataset:
        disturbance_read_cols = list(
            set(self._disturbance_dataset.get_layer_names())
            - {"disturbance_id", "timestep", "default_disturbance_type_id", "disturbance_order"}
        )

        flat_layers = []
        for partition in self._disturbance_dataset.get_partition_values():
            # Need to split original disturbance dataset up into a dataset per
            # partition: GCBMDisturbancePreprocessor expects dataset-wide unique
            # pixel values mapped to an attribute table. "index" will be the
            # pixel value, but it is only unique within a partition.
            read_filters = [[k, "=", v] for k, v in partition.items()]
            partition_name_part = "_".join((str(v) for v in partition.values()))
            out_ds_name = f"base_disturbance_{partition_name_part}"
            split_output_ds = self._disturbance_dataset.create_new(
                out_ds_name,
                "local_storage",
                str(Path(self._temp_dir.name).joinpath(out_ds_name)),
                copy_raster_index_data=False,
                partitions={"chunk_index": "int32"},
                tags=pd.DataFrame({
                    "layer_name": [out_ds_name],
                    "tag": ["disturbance"],
                }),
            )
            
            data = self._disturbance_dataset.read_pandas(
                filters=read_filters,
                read_cols=disturbance_read_cols,
            )

            data["id"] = data["index"]
            split_output_ds.write(data)
            split_output_ds.write(
                self._disturbance_dataset.read_pandas(
                    self._disturbance_dataset.raster_index_table_name,
                    filters=read_filters,
                    read_cols=["chunk_index", "index", "raster_index"]
                ),
                split_output_ds.raster_index_table_name
            )

            flat_output_ds_path = str(
                Path(self._temp_dir.name).joinpath(f"{out_ds_name}_flat")
            )

            flat_output_ds = to_single_layer_flattened_dataset(
                out_ds_name,
                "local_storage",
                flat_output_ds_path,
                split_output_ds,
                [
                    "id", "year", "disturbance_type", "disturbed_transition_id",
                    "undisturbed_transition_id", "sort_id", "filter_id", "proportion"
                ]
            )

            flat_output_ds.meta.write_tags(pd.DataFrame(
                {"layer_name": [out_ds_name], "tag": ["disturbance"]}
            ))

            # Re-instantiate the dataset to ensure "disturbance" metadata tag
            # is refreshed.
            flat_layers.append(
                FlattenedCoordinateDataset(
                    flat_output_ds.name, "local_storage", flat_output_ds_path
                )
            )

        output_layer_collection = InputLayerCollection(
            [
                FlattenedCoordinateInputLayer(
                    layer,
                    layer.get_layer_names(),
                )
                for layer in flat_layers
            ]
        )

        output_ds = flattened_coordinate_dataset.create(
            output_layer_collection,
            "disturbance",
            "local_storage",
            str(Path(self._temp_dir.name).joinpath("merged_disturbances")),
            creation_options={
                "chunk_options": {
                    "chunk_x_size_max": self._disturbance_dataset.chunks[0].x_size,
                    "chunk_y_size_max": self._disturbance_dataset.chunks[0].y_size,
                },
                "max_workers": 1,
            }
        )

        # Drop unwanted columns that are hard to remove before this point.
        for layer_name in output_ds.get_layer_names():
            attribute_table = output_ds.meta.get_attribute_table(layer_name).drop(
                columns=["chunk_index", "index"]
            )

            output_ds.meta.write_attribute_table(layer_name, attribute_table)

        for table_name in self._disturbance_dataset.list_tables():
            output_ds.write_table(
                table_name,
                self._disturbance_dataset.read_table_pandas(table_name)
            )

        for _, file_or_dir_name in self._disturbance_dataset.list_files_and_dirs():
            extracted_path = str(Path(self._temp_dir.name).joinpath(file_or_dir_name))
            self._disturbance_dataset.extract_file_or_dir(file_or_dir_name, extracted_path)
            output_ds.write_file_or_dir(file_or_dir_name, extracted_path)

        return output_ds

    def _get_dataset_config(self, name: str) -> dict[str, Any]:
        config = self._cbm4_config["cbm4_spatial_dataset"][name].copy()
        config["path_or_uri"] = str(self._cbm4_config.resolve(config["path_or_uri"]))

        return config

    def _get_dataset(self, name: str) -> RasterIndexedDataset:
        dataset_config = self._get_dataset_config(name)
        return RasterIndexedDataset(
            dataset_config["dataset_name"],
            dataset_config["storage_type"],
            dataset_config["path_or_uri"],
        )
