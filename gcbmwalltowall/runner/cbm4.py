from __future__ import annotations

import json
import os
import shutil
import time
from typing import Any

import pandas as pd
from cbm4.app.spatial.spatial_cbm3.spatial_cbm3_app import (
    create_simulation_dataset, spinup_all, step_all)
from cbm4.app.spatial.event_handler.event_processor import EventProcessor
from gcbmwalltowall.util.path import Path


def load_config(
    cbm4_config_path: str | Path,
    max_workers: int = None,
    **kwargs,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

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

    return simulation_config, spinup_config, step_configs


def run(cbm4_config_path: str | Path, **kwargs):
    json_config = json.load(open(cbm4_config_path))
    sim_start_year = int(json_config["start_year"])

    simulation_config, spinup_config, step_configs = load_config(
        cbm4_config_path, **kwargs
    )

    cbm4_root = os.path.join(
        simulation_config["out_simulation_dataset"]["path_or_uri"],
        ".."
    )

    shutil.rmtree(simulation_config["out_simulation_dataset"]["path_or_uri"], True)

    step_times = []
    start = time.time()
    create_simulation_dataset(simulation_config)
    step_times.append(["create simulation dataset", (time.time() - start)])

    start = time.time()
    spinup_all(spinup_config)
    step_times.append(["spinup", (time.time() - start)])

    event_processor = EventProcessor.for_simulation(cbm4_root)
    for step_config in step_configs:
        start = time.time()
        event_processor.process_events_for_timestep(step_config["timestep"])
        step_all(step_config)
        step_times.append(
            [f"timestep_{step_config['timestep']}", (time.time() - start)]
        )

    time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
    time_profiling.to_csv(
        Path(cbm4_config_path).absolute().parent.joinpath("profiling.csv"), index=False
    )
