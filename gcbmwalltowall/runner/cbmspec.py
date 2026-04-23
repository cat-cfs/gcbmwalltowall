from __future__ import annotations

import json
import os
import shutil
import time
from tempfile import TemporaryDirectory
from typing import Any, Callable

import pandas as pd
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from cbm4.app.spatial.spatial_cbm4 import cbm4_spatial_runner
from cbm4.app.spatial.event_handler.event_processor import EventProcessor
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix
from cbmspec_cbm3.parameters.cbm_defaults import cbm4_parameter_dataset_factory

from gcbmwalltowall.util.path import Path


def load_config(cbm4_config_path: str | Path, **kwargs) -> dict[str, Any]:
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

    return json_config


def create_model(inventory_ds: RasterIndexedDataset, **model_kwargs) -> Any:
    with TemporaryDirectory() as tmp:
        cbm_defaults_path = Path(tmp).joinpath("cbm_defaults.db")
        try:
            inventory_ds.extract_file_or_dir("cbm_defaults", str(cbm_defaults_path))
        except:
            pass

        cbmspec_cbm3_single_matrix_model = cbmspec_cbm3_single_matrix.model_create(
            str(cbm_defaults_path) if cbm_defaults_path.exists() else None,
            **model_kwargs
        )

    return cbmspec_cbm3_single_matrix_model


def run(
    cbm4_config_path: str | Path,
    max_workers: int = None,
    write_parameters: bool = False,
    on_pre_spinup: Callable[[str]] | None = None,
    on_pre_simulation: Callable[[str]] | None = None,
    **kwargs,
):
    json_config = kwargs.get("json_config") or load_config(cbm4_config_path, **kwargs)
    shutil.rmtree(
        json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"], True
    )

    inventory_ds = RasterIndexedDataset(
        json_config["cbm4_spatial_dataset"]["inventory"]["dataset_name"],
        json_config["cbm4_spatial_dataset"]["inventory"]["storage_type"],
        json_config["cbm4_spatial_dataset"]["inventory"]["path_or_uri"],
    )

    spinup_model_config = json_config.get("model_parameters", {}).get("spinup", {})
    step_model_config = json_config.get("model_parameters", {}).get("step", {})
    for model_config in (spinup_model_config, step_model_config):
        increment_table_rel_path = model_config.get("increment_table")
        if increment_table_rel_path:
            increment_table_abs_path = os.path.join(
                os.path.dirname(cbm4_config_path),
                increment_table_rel_path
            )

            model_config["increment_table"] = increment_table_abs_path

    spinup_model = create_model(
        inventory_ds,
        **json_config.get("model_parameters", {}).get("spinup", {})
    )

    step_model = json_config.get("cbmspec_model") or create_model(
        inventory_ds,
        **json_config.get("model_parameters", {}).get("step", {})
    )

    step_times = []
    start = time.time()
    simulation_ds = cbm4_spatial_runner.create_simulation_dataset(
        spinup_model,
        inventory_ds,
        json_config["cbm4_spatial_dataset"]["simulation"]["dataset_name"],
        json_config["cbm4_spatial_dataset"]["simulation"]["storage_type"],
        json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"],
    )
    step_times.append(["create simulation dataset", (time.time() - start)])

    out_path = Path(
        json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"]
    ).parent

    spinup_spatial_parameter_ds = (
        cbm4_parameter_dataset_factory.spinup_parameter_dataset_create(
            inventory_ds,
            "spinup_parameters",
            "local_storage",
            str(out_path.joinpath("spinup_parameters")),
            enable_cbm_cfs3_smoother=json_config.get("use_smoother", True),
        )
    )

    step_spatial_parameter_ds = (
        cbm4_parameter_dataset_factory.step_parameter_dataset_create(
            inventory_ds,
            "step_parameters",
            "local_storage",
            str(out_path.joinpath("step_parameters")),
            enable_cbm_cfs3_smoother=json_config.get("use_smoother", True),
        )
    )

    if on_pre_spinup is not None:
        start = time.time()
        on_pre_spinup(json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"])
        step_times.append(["pre-spinup callback", (time.time() - start)])

    start = time.time()
    cbm4_spatial_runner.spinup_all(
        model=spinup_model,
        inventory_dataset=inventory_ds,
        simulation_dataset=simulation_ds,
        parameter_dataset=spinup_spatial_parameter_ds,
        max_workers=max_workers,
        write_parameters=write_parameters,
    )
    step_times.append(["spinup", (time.time() - start)])

    if on_pre_simulation is not None:
        start = time.time()
        on_pre_simulation(json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"])
        step_times.append(["pre-simulation callback", (time.time() - start)])

    final_timestep = json_config["end_year"] - json_config["start_year"] + 1
    with TemporaryDirectory() as tmp:
        event_processor = EventProcessor.for_simulation(str(out_path.absolute()), tmp)
        for timestep in range(1, final_timestep + 1):
            start = time.time()
            disturbance_ds = event_processor.process_events_for_timestep(timestep)
            cbm4_spatial_runner.step_all(
                model=step_model,
                timestep=timestep,
                simulation_input_dataset=simulation_ds,
                disturbance_event_dataset=disturbance_ds,
                simulation_output_dataset=simulation_ds,
                parameter_dataset=step_spatial_parameter_ds,
                area_unit_conversion=0.0001,
                max_workers=max_workers,
                write_parameters=write_parameters,
            )
            step_times.append([f"timestep_{timestep}", (time.time() - start)])

        time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
        time_profiling.to_csv(out_path.joinpath("profiling.csv"), index=False)
