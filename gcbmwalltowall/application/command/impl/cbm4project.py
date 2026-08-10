from tempfile import TemporaryDirectory
from pathlib import Path
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from arrow_space.operations.export.geotiff_export import GeoTiffExporter
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
    def inventory_dataset(self) -> RasterIndexedDataset:
        return self._inventory_dataset

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
    def chunk_size(self) -> tuple[int, int]:
        return (
            self._inventory_dataset.chunks[0].x_size,
            self._inventory_dataset.chunks[0].y_size
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
                    self._disturbance_dataset.read_table_pandas(
                        table_name
                    )["id"].astype("int").max()
                )

        return max_transition_id

    def extract_bounding_box(self) -> str:
        if self._bbox_path is not None:
            return self._bbox_path

        inv_data = self._inventory_dataset.read_polars()
        inv_ri_data = self._inventory_dataset.read_polars(
            self._inventory_dataset.raster_index_table_name
        )

        export_data = inv_data.join(
            inv_ri_data, on=["chunk_index", "index", "cohort_index"]
        ).select(
            "chunk_index", "raster_index"
        ).unique().with_columns(bbox=1).collect().to_pandas()

        exporter = GeoTiffExporter(self._inventory_dataset)
        exporter.write_data(export_data)
        exporter.write_geotiff(self._temp_dir.name, "GTiff")
        self._bbox_path = str(Path(self._temp_dir.name).joinpath("bbox.tiff"))

        return self._bbox_path

    def _get_dataset(self, name: str) -> RasterIndexedDataset:
        dataset_config = self._cbm4_config["cbm4_spatial_dataset"][name]
        return RasterIndexedDataset(
            dataset_config["dataset_name"],
            dataset_config["storage_type"],
            str(self._cbm4_config.resolve(dataset_config["path_or_uri"]))
        )
