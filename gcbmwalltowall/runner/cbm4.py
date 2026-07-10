from __future__ import annotations

import json
import os
import shutil
import time
import pandas as pd
from typing import Any, Callable
from tempfile import TemporaryDirectory
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from cbm4.app.spatial.spatial_cbm3.spatial_cbm3_app import (
    create_simulation_dataset, spinup_all, step_all)
from cbm4.app.spatial.event_handler.event_processor import EventProcessor
from gcbmwalltowall.util.path import Path
from tqdm import tqdm


def load_config(
    cbm4_config_path: str | Path,
    max_workers: int | None = None,
    end_year: int | None = None,
    **kwargs,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

    cache_config = json_config.get("cache")
    if cache_config is not None:
        relative_path = cache_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        cache_config["path_or_uri"] = absolute_path

    simulation_config = {
        "inventory_dataset": json_config["cbm4_spatial_dataset"]["inventory"],
        "disturbance_dataset": json_config["cbm4_spatial_dataset"]["disturbance"],
        "out_simulation_dataset": json_config["cbm4_spatial_dataset"]["simulation"],
    }

    spinup_config = {
        "inventory_dataset": json_config["cbm4_spatial_dataset"]["inventory"],
        "simulation_dataset": json_config["cbm4_spatial_dataset"]["simulation"],
        "max_workers": max_workers,
        "cbm_defaults_locale": json_config.get("cbm_defaults_locale", "en-CA"),
        "use_smoother": json_config.get("use_smoother", True),
    }

    project_start_year = json_config["start_year"]
    project_end_year = end_year or json_config["end_year"]
    sim_start_year = project_start_year if cache_config is None else (cache_config["end_year"] + 1)
    sim_start_timestep = sim_start_year - project_start_year + 1
    sim_end_timestep = project_end_year - project_start_year + 1
    step_configs = [
        {
            "timestep": timestep,
            "simulation_dataset": (
                {
                    k: v for k, v in cache_config.items()
                    if k in ("dataset_name", "storage_type", "path_or_uri")
                } if i == 0 and cache_config
                else json_config["cbm4_spatial_dataset"]["simulation"]
            ),
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
        for i, timestep in enumerate(range(sim_start_timestep, sim_end_timestep + 1))
    ]

    return simulation_config, spinup_config, step_configs


def run(
    cbm4_config_path: str | Path,
    on_pre_spinup: Callable[[str]] | None = None,
    on_pre_simulation: Callable[[str]] | None = None,
    end_year: int | None = None,
    **kwargs
):
    simulation_config, spinup_config, step_configs = load_config(
        cbm4_config_path, end_year=end_year, **kwargs
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

    if on_pre_spinup is not None:
        start = time.time()
        on_pre_spinup(simulation_config["out_simulation_dataset"]["path_or_uri"])
        step_times.append(["pre-spinup callback", (time.time() - start)])

    spinup_cached = json.load(open(cbm4_config_path)).get("cache") is not None
    with tqdm(desc="Simulation", total=len(step_configs) + (0 if spinup_cached else 1)) as pbar:
        if not spinup_cached:
            start = time.time()
            spinup_all(spinup_config)
            pbar.update()
            step_times.append(["spinup", (time.time() - start)])

        if on_pre_simulation is not None:
            start = time.time()
            on_pre_simulation(simulation_config["out_simulation_dataset"]["path_or_uri"])
            step_times.append(["pre-simulation callback", (time.time() - start)])

        with TemporaryDirectory() as tmp:
            # Create a temporary working copy of the disturbance dataset to be used
            # by both rule-based EventProcessor.
            working_disturbance_ds_path = Path(tmp).joinpath("disturbance")
            RasterIndexedDataset(
                simulation_config["disturbance_dataset"]["dataset_name"],
                simulation_config["disturbance_dataset"]["storage_type"],
                simulation_config["disturbance_dataset"]["path_or_uri"]
            ).copy("disturbance", "local_storage", str(working_disturbance_ds_path))
            
            working_disturbance_ds = RasterIndexedDataset(
                "disturbance", "local_storage", str(working_disturbance_ds_path)
            )

            simulation_ds = RasterIndexedDataset(
                simulation_config["out_simulation_dataset"]["dataset_name"],
                simulation_config["out_simulation_dataset"]["storage_type"],
                simulation_config["out_simulation_dataset"]["path_or_uri"]
            )

            t0_event_processor = None
            event_processor = EventProcessor.for_datasets(simulation_ds, working_disturbance_ds)
            for i, step_config in enumerate(step_configs):
                start = time.time()
                if i == 0 and spinup_cached:
                    t0_simulation_ds = RasterIndexedDataset(
                        step_config["simulation_dataset"]["dataset_name"],
                        step_config["simulation_dataset"]["storage_type"],
                        step_config["simulation_dataset"]["path_or_uri"],
                    )

                    t0_event_processor = EventProcessor.for_datasets(
                        t0_simulation_ds, working_disturbance_ds
                    )
                    
                    t0_event_processor.process_events_for_timestep(step_config["timestep"])
                else:
                    event_processor.process_events_for_timestep(step_config["timestep"])
                
                step_config["disturbance_dataset"]["path_or_uri"] = str(working_disturbance_ds_path)
                step_all(step_config)
                pbar.update()
                step_times.append(
                    [f"timestep_{step_config['timestep']}", (time.time() - start)]
                )

        if t0_event_processor is not None:
            t0_event_processor.summarize(os.path.join(cbm4_root, "event_processor_summary_t0.csv"))

        event_processor.summarize(os.path.join(cbm4_root, "event_processor_summary.csv"))
        time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=step_times)
        time_profiling.to_csv(
            Path(cbm4_config_path).absolute().parent.joinpath("profiling.csv"), index=False
        )
