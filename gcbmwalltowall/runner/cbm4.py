from __future__ import annotations
import json
import os
import time
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from pydantic import BaseModel
from cbm4.app.spatial.spatial_cbm3 import cbm3_spatial_runner
from cbm4.app.spatial.spatial_cbm3.area_calculator import EqualAreaCalculator, WGS84AreaCalculator
from cbm4.app.spatial.gcbm_input import gcbm_preprocessor
from cbm4.app.spatial.gcbm_input.timestep_interpreter import YearOffsetTimestepInterpreter
from cbm4.app.spatial.gcbm_input.timestep_interpreter import TimestepInterpreter
from cbm4.app.spatial.gcbm_input.disturbance_event_sorter import DefaultDisturbanceTypeIdSorter
from cbm4.app.spatial.gcbm_input.disturbance_event_sorter import DisturbanceEventSorter
from gcbmwalltowall.component.preparedproject import PreparedProject
from gcbmwalltowall.converter.projectconverter import ProjectConverter
from arrow_space.storage.variable_storage_type import VariableStorageType
from arrow_space.raster_indexed_dataset import RasterIndexedDataset


class DataSetInfo(BaseModel):
    dataset_name: str
    storage_type: str | VariableStorageType
    path_or_uri: str


class CBM4SpatialDatasetInfo(BaseModel):
    inventory: DataSetInfo
    disturbance: DataSetInfo
    simulation: DataSetInfo


class PreprocessModel(BaseModel):
    wall_to_wall_project_path: str
    output_path: str
    is_converted_project: bool
    resolution: float
    cbm4_spatial_dataset: CBM4SpatialDatasetInfo
    default_inventory_values: dict[str, Any]
    start_year: int
    end_year: int
    disturbance_event_sorter: None


class CBM4SpatialDataset:
    def __init__(self, cbm4_spatial_dataset: CBM4SpatialDatasetInfo | dict):
        if isinstance(cbm4_spatial_dataset, dict):
            cbm4_spatial_dataset = CBM4SpatialDatasetInfo.model_validate(cbm4_spatial_dataset)
        
        self._inventory = RasterIndexedDataset(
            dataset_name=cbm4_spatial_dataset.inventory.dataset_name,
            storage_type=cbm4_spatial_dataset.inventory.storage_type,
            storage_path_or_uri=cbm4_spatial_dataset.inventory.path_or_uri)

        self._disturbance = RasterIndexedDataset(
            dataset_name=cbm4_spatial_dataset.disturbance.dataset_name,
            storage_type=cbm4_spatial_dataset.disturbance.storage_type,
            storage_path_or_uri=cbm4_spatial_dataset.disturbance.path_or_uri)

        self._simulation = RasterIndexedDataset(
            dataset_name=cbm4_spatial_dataset.simulation.dataset_name,
            storage_type=cbm4_spatial_dataset.simulation.storage_type,
            storage_path_or_uri=cbm4_spatial_dataset.simulation.path_or_uri)

    @property
    def inventory(self) -> RasterIndexedDataset:
        return self._inventory

    @property
    def disturbance(self) -> RasterIndexedDataset:
        return self._disturbance

    @property
    def simulation(self) -> RasterIndexedDataset:
        return self._simulation


def _preprocess(
    wall_to_wall_project_path: str,
    is_converted_project: bool,
    cbm4_spatial_dataset: CBM4SpatialDatasetInfo | dict,
    disturbance_timestep_interpreter: TimestepInterpreter,
    disturbance_event_sorter: DisturbanceEventSorter,
    default_inventory_values: dict[str, Any] | None = None,
):
    if isinstance(cbm4_spatial_dataset, dict):
        cbm4_spatial_dataset = CBM4SpatialDatasetInfo.model_validate(cbm4_spatial_dataset)

    time_profiling = []
    start = time.time()
    if not is_converted_project:
        tempdir = TemporaryDirectory()
        converted_datadir = os.path.join(tempdir.name, "wall_to_wall_converted")
        os.makedirs(converted_datadir)
        project = PreparedProject(wall_to_wall_project_path)
        converter = ProjectConverter()
        converter.convert(project, converted_datadir)
        time_profiling.append(["walltowall convert", (time.time() - start)])
    else:
        converted_datadir = wall_to_wall_project_path

    start = time.time()
    gcbm_preprocessor.preprocess_inventory(
        converted_datadir,
        out_dataset_name=cbm4_spatial_dataset.inventory.dataset_name,
        out_storage_type=cbm4_spatial_dataset.inventory.storage_type,
        out_storage_path_or_uri=cbm4_spatial_dataset.inventory.path_or_uri,
        area_unit_conversion=0.0001,
        override_values=default_inventory_values)

    time_profiling.append(["cbm4 preprocess_inventory", (time.time() - start)])

    start = time.time()
    gcbm_preprocessor.preprocess_disturbance(
        converted_datadir,
        out_dataset_name=cbm4_spatial_dataset.disturbance.dataset_name,
        out_storage_type=cbm4_spatial_dataset.disturbance.storage_type,
        out_storage_path_or_uri=cbm4_spatial_dataset.disturbance.path_or_uri,
        timestep_interpreter=disturbance_timestep_interpreter,
        disturbance_event_sorter=disturbance_event_sorter)

    time_profiling.append(["cbm4 preprocess_disturbance", (time.time() - start)])

    start = time.time()
    cbm3_spatial_runner.create_simulation_dataset(
        inventory_dataset=RasterIndexedDataset(
            cbm4_spatial_dataset.inventory.dataset_name,
            cbm4_spatial_dataset.inventory.storage_type,
            cbm4_spatial_dataset.inventory.path_or_uri),
        out_dataset_name=cbm4_spatial_dataset.simulation.dataset_name,
        out_storage_type=cbm4_spatial_dataset.simulation.storage_type,
        out_storage_path_or_uri=cbm4_spatial_dataset.simulation.path_or_uri)

    time_profiling.append(["cbm4 create_simulation_dataset", (time.time() - start)])

    log_path = Path(cbm4_spatial_dataset.simulation.path_or_uri).parent
    pd.DataFrame(columns=["task", "time_elapsed"], data=time_profiling).to_csv(
        str(log_path.joinpath("preprocess_profiling.csv")), index=False)


def preprocess(preprocess_arg: PreprocessModel):
    _preprocess(
        wall_to_wall_project_path=preprocess_arg.wall_to_wall_project_path,
        is_converted_project=preprocess_arg.is_converted_project,
        cbm4_spatial_dataset=preprocess_arg.cbm4_spatial_dataset,
        default_inventory_values=preprocess_arg.default_inventory_values,
        disturbance_timestep_interpreter=YearOffsetTimestepInterpreter(preprocess_arg.start_year - 1),
        disturbance_event_sorter=DefaultDisturbanceTypeIdSorter(),
    )


def spinup(config: PreprocessModel):
    start = time.time()
    spatial_dataset = CBM4SpatialDataset(config.cbm4_spatial_dataset)
    cbm3_spatial_runner.spinup_all(
        inventory_dataset=spatial_dataset.inventory,
        simulation_dataset=spatial_dataset.simulation)

    time_profiling = pd.DataFrame(
        columns=["task", "time_elapsed"],
        data=[["spinup", (time.time() - start)]])

    time_profiling.to_csv(str(Path(config.output_path).joinpath("spinup_time.csv")), index=False)


def step(config: PreprocessModel, timestep: int):
    spatial_dataset = CBM4SpatialDataset(config.cbm4_spatial_dataset)
    ha_per_m2 = 0.0001
    area_calculator = (
        WGS84AreaCalculator(ha_per_m2) if config.resolution < 1
        else EqualAreaCalculator(config.resolution**2 * ha_per_m2)
    )

    cbm3_spatial_runner.step_all(
        timestep,
        spatial_dataset.simulation,
        spatial_dataset.disturbance,
        spatial_dataset.simulation,
        area_calculator)


def load_config(cbm4_config_path: str | Path):
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    json_config["output_path"] = output_path
    if "wall_to_wall_project_path" not in json_config:
        json_config["is_converted_project"] = True
        json_config["wall_to_wall_project_path"] = str(cbm4_config_path.parent)
    else:
        json_config["is_converted_project"] = False
    
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

    cbm4_config = PreprocessModel.model_validate(json_config)
    
    return cbm4_config


def run(cbm4_config_path: str | Path):
    cbm4_config = load_config(cbm4_config_path)
    preprocess(cbm4_config)
    spinup(cbm4_config)
    step_times = []
    final_timestep = cbm4_config.end_year - cbm4_config.start_year + 1
    for timestep in range(1, final_timestep + 1):
        start = time.time()
        step(cbm4_config, timestep)
        step_times.append([f"timestep_{timestep}", (time.time() - start)])

    time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
    time_profiling.to_csv(os.path.join(cbm4_config.output_path, "step_time.csv"), index=False)
