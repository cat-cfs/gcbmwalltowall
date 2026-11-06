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
        return RasterIndexedDataset(**self.constructor_args)

    @cached_property
    def constructor_args(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "storage_type": self.storage_type,
            "storage_path_or_uri": str(self.path_or_uri),
        }

    @property
    def exists(self) -> bool:
        return Path(self.path_or_uri).exists()

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
    def model(self) -> Any:
        cbmspec_model = (
            self.config.get("cbmspec_model")
            or cbmspec_cbm3_single_matrix.model_create(
                self.config.get("cbm_defaults_path"),
                self.config.get("locale", "en-CA"),
                self.config.get("increment_table"),
            )
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
