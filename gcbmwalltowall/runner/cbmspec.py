from __future__ import annotations
import json
import os
import time
import shutil
import pandas as pd
from pathlib import Path
from cbm4.app.spatial.gcbm_input.gcbm_preprocessor_app import preprocess
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from cbm4.app.spatial.spatial_cbm4 import cbm4_spatial_runner
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix
from cbmspec_cbm3.parameters.cbm_defaults import cbm4_parameter_dataset_factory


def load_config(
    cbm4_config_path: str | Path,
    max_workers: int = None,
    apply_departial_dms: bool = False,
    **kwargs,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

    preprocess_config = {
        "data_dir": output_path,
        "inventory_dataset": json_config["cbm4_spatial_dataset"]["inventory"],
        "disturbance_dataset": json_config["cbm4_spatial_dataset"]["disturbance"],
        "timestep_interpreter": {
            "type": "year_offset",
            "year_offset": json_config["start_year"] - 1,
        },
        "disturbance_event_sorter": {
            "type": "list",
            "sort_order": json_config["disturbance_order"],
        },
        "area_unit_conversion": 0.0001, # ha/m^2
        "cbm_defaults_locale": json_config.get("cbm_defaults_locale", "en-CA"),
        "inventory_override_values": json_config.get("default_inventory_values"),
        "max_workers": max_workers,
        "apply_departial_dms": apply_departial_dms,
    }

    return preprocess_config, json_config


def run(cbm4_config_path: str | Path, max_workers: int = None, **kwargs):
    preprocess_config, json_config = load_config(
        cbm4_config_path, max_workers, **kwargs
    )

    for dataset_info in (
        preprocess_config["inventory_dataset"],
        preprocess_config["disturbance_dataset"],
        json_config["cbm4_spatial_dataset"]["simulation"],
    ):
        shutil.rmtree(dataset_info["path_or_uri"], True)

    step_times = []
    start = time.time()
    preprocess(preprocess_config)
    step_times.append(["preprocess", (time.time() - start)])

    inventory_ds = RasterIndexedDataset(
        preprocess_config["inventory_dataset"]["dataset_name"],
        preprocess_config["inventory_dataset"]["storage_type"],
        preprocess_config["inventory_dataset"]["path_or_uri"]
    )

    disturbance_ds = RasterIndexedDataset(
        preprocess_config["disturbance_dataset"]["dataset_name"],
        preprocess_config["disturbance_dataset"]["storage_type"],
        preprocess_config["disturbance_dataset"]["path_or_uri"]
    )

    cbmspec_cbm3_single_matrix_model = cbmspec_cbm3_single_matrix.model_create(
        str(Path(cbm4_config_path).parent.joinpath("cbm_defaults.db"))
    )

    start = time.time()
    simulation_ds = cbm4_spatial_runner.create_simulation_dataset(
        cbmspec_cbm3_single_matrix_model,
        inventory_ds,
        json_config["cbm4_spatial_dataset"]["simulation"]["dataset_name"],
        json_config["cbm4_spatial_dataset"]["simulation"]["storage_type"],
        json_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"]
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
            use_smoother=json_config.get("use_smoother", True)
        )
    )

    step_spatial_parameter_ds = (
        cbm4_parameter_dataset_factory.step_parameter_dataset_create(
            inventory_ds,
            "step_parameters",
            "local_storage",
            str(out_path.joinpath("step_parameters")),
            use_smoother=json_config.get("use_smoother", True)
        )
    )

    start = time.time()
    cbm4_spatial_runner.spinup_all(
        model=cbmspec_cbm3_single_matrix_model,
        inventory_dataset=inventory_ds,
        simulation_dataset=simulation_ds,
        parameter_dataset=spinup_spatial_parameter_ds,
        max_workers=max_workers,
    )
    step_times.append(["spinup", (time.time() - start)])

    final_timestep = json_config["end_year"] - json_config["start_year"] + 1
    for timestep in range(1, final_timestep + 1):
        start = time.time()
        cbm4_spatial_runner.step_all(
            model=cbmspec_cbm3_single_matrix_model,
            timestep=timestep,
            simulation_input_dataset=simulation_ds,
            disturbance_event_dataset=disturbance_ds,
            simulation_output_dataset=simulation_ds,
            parameter_dataset=step_spatial_parameter_ds,
            area_unit_conversion=0.0001,
            max_workers=max_workers,
        ) 
        step_times.append(
            [f"timestep_{timestep}", (time.time() - start)]
        )

    time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
    time_profiling.to_csv(
        os.path.join(preprocess_config["data_dir"], "profiling.csv"), index=False
    )
