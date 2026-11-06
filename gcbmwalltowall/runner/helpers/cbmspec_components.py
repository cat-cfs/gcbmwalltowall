from __future__ import annotations
import pathlib
from functools import cached_property
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any, Callable
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix
from gcbmwalltowall.util.path import Path
from arrow_space.storage.variable_storage_type import VariableStorageType


@dataclass
class DatasetConfig:
    dataset_name: str
    path_or_uri: str | pathlib.Path
    storage_type: VariableStorageType = "local_storage"

    @property
    def dataset(self) -> RasterIndexedDataset:
        return RasterIndexedDataset(**self.to_constructor_args())

    @property
    def exists(self) -> bool:
        return Path(self.path_or_uri).exists()

    def to_constructor_args(self, prefix: str = "") -> dict[str, Any]:
        if prefix:
            prefix += "_"
        
        return {
            f"{prefix}dataset_name": self.dataset_name,
            f"{prefix}storage_type": self.storage_type,
            f"{prefix}storage_path_or_uri": str(self.path_or_uri),
        }

    def copy(self, dest_dir: str | pathlib.Path) -> DatasetConfig:
        out_path = Path(dest_dir).joinpath(self.dataset_name)
        self.dataset.copy(self.dataset_name, self.storage_type, str(out_path))

        return DatasetConfig(self.dataset_name, out_path, self.storage_type)


class CBMDefaultsConfig:

    def __init__(self, inventory_ds: DatasetConfig, locale="en-CA"):
        self.locale = locale
        ds = inventory_ds.dataset
        if ds.file_or_dir_exists("cbm_defaults"):
            self._tmp = TemporaryDirectory()
            self.path = Path(self._tmp.name).joinpath("cbm_defaults.db")
            ds.extract_file_or_dir("cbm_defaults", str(self.path))
        else:
            self.path = None


@dataclass
class CBMSpecModelConfig:
    config: dict

    @property
    def constructor_args(self):
        return {
            k: v for k, v in self.config.items()
            if k in (
                "cbm_defaults_path",
                "locale",
                "increment_table",
                "historic_increment_table"
            )
        }

    @property
    def model(self) -> Any:
        cbmspec_model = (
            self.config.get("cbmspec_model")
            or cbmspec_cbm3_single_matrix.model_create(**self.constructor_args)
        )
        
        return cbmspec_model


@dataclass
class CBMSpecCallbacks:
    # Legacy: callbacks receive simulation dataset path only:
    legacy_on_pre_spinup: Callable[[str], None] | None = None
    legacy_on_pre_simulation: Callable[[str], None] | None = None

    # New: callbacks receive whole CBMSpecSpinup or CBMSpecStepping:
    on_pre_spinup: Callable[[CBMSpecSpinup], None] | None = None
    on_pre_simulation: Callable[[CBMSpecStepping], None] | None = None


@dataclass
class CBMSpecCache:
    end_year: int
    simulation_output_ds: DatasetConfig
    step_parameter_ds: DatasetConfig
