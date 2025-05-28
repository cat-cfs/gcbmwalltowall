from __future__ import annotations
import json
import os
import time
import shutil
import pandas as pd
from pathlib import Path
from cbm4.app.spatial.spatial_cbm3 import cbm3_spatial_runner
from cbm4.app.spatial.gcbm_input.gcbm_preprocessor_app import preprocess
from cbm4.app.spatial.spatial_cbm3.spatial_cbm3_app import create_simulation_dataset
from cbm4.app.spatial.spatial_cbm3.spatial_cbm3_app import spinup_all
from cbm4.app.spatial.spatial_cbm3.spatial_cbm3_app import step_all


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
        "area_unit_conversion": 0.0001,  # ha/m^2
        "cbm_defaults_locale": json_config.get("cbm_defaults_locale", "en-CA"),
        "inventory_override_values": json_config.get("default_inventory_values"),
        "max_workers": max_workers,
        "apply_departial_dms": apply_departial_dms,
    }

    simulation_config = {
        "inventory_dataset": json_config["cbm4_spatial_dataset"]["inventory"],
        "out_simulation_dataset": json_config["cbm4_spatial_dataset"]["simulation"],
    }

    spinup_config = {
        "inventory_dataset": json_config["cbm4_spatial_dataset"]["inventory"],
        "simulation_dataset": json_config["cbm4_spatial_dataset"]["simulation"],
        "max_workers": max_workers,
        "cbm_defaults_locale": json_config.get("cbm_defaults_locale", "en-CA"),
        "use_smoother": json_config.get("use_smoother", True),
    }

    final_timestep = json_config["end_year"] - json_config["start_year"] + 1
    step_configs = [
        {
            "timestep": timestep,
            "simulation_dataset": json_config["cbm4_spatial_dataset"]["simulation"],
            "disturbance_dataset": json_config["cbm4_spatial_dataset"]["disturbance"],
            "simulation_output_dataset": json_config["cbm4_spatial_dataset"][
                "simulation"
            ],
            "area_unit_conversion": 0.0001,  # ha/m^2
            "cbm_defaults_locale": json_config.get("cbm_defaults_locale", "en-CA"),
            "disturbance_output_reporting_cols": json_config.get(
                "disturbance_output_reporting_cols"
            ),
            "max_workers": max_workers,
            "use_smoother": json_config.get("use_smoother", True),
        }
        for timestep in range(1, final_timestep + 1)
    ]

    return preprocess_config, simulation_config, spinup_config, step_configs


def run(cbm4_config_path: str | Path, **kwargs):
    preprocess_config, simulation_config, spinup_config, step_configs = load_config(
        cbm4_config_path, **kwargs
    )
    for dataset_info in (
        preprocess_config["inventory_dataset"],
        preprocess_config["disturbance_dataset"],
        simulation_config["out_simulation_dataset"],
    ):
        shutil.rmtree(dataset_info["path_or_uri"], True)

    step_times = []

    start = time.time()
    preprocess(preprocess_config)
    step_times.append(["preprocess", (time.time() - start)])

    start = time.time()
    create_simulation_dataset(simulation_config)
    step_times.append(["create simulation dataset", (time.time() - start)])

    start = time.time()
    spinup_all(spinup_config)
    step_times.append(["spinup", (time.time() - start)])

    for step_config in step_configs:
        start = time.time()
        step_all(step_config)
        step_times.append(
            [f"timestep_{step_config['timestep']}", (time.time() - start)]
        )

    time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
    time_profiling.to_csv(
        os.path.join(preprocess_config["data_dir"], "profiling.csv"), index=False
    )
