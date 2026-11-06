from __future__ import annotations
import json
import os
import shutil
import time
import pathlib
from tqdm import tqdm
from typing import Any
from cbmspec_cbm3.parameters.cbm_defaults import cbm4_parameter_dataset_factory
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMDefaultsConfig
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecCache
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecCallbacks
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecModelConfig
from gcbmwalltowall.runner.helpers.cbmspec_components import DatasetConfig
from gcbmwalltowall.runner.helpers.cbmspec_run import CBMSpecRun
from gcbmwalltowall.runner.helpers.cbmspec_run import CBMSpecSpinup
from gcbmwalltowall.runner.helpers.cbmspec_run import CBMSpecStepping
from gcbmwalltowall.runner.helpers.cbmspec_run import timed
from gcbmwalltowall.util.path import Path


def load_config(cbm4_config_path: str | pathlib.Path, **kwargs) -> dict[str, Any]:
    output_path = str(Path(cbm4_config_path).absolute().parent)
    json_config = json.load(open(cbm4_config_path))
    for _, dataset_config in json_config["cbm4_spatial_dataset"].items():
        relative_path = dataset_config["path_or_uri"]
        absolute_path = os.path.join(output_path, relative_path)
        dataset_config["path_or_uri"] = absolute_path

    cache_config = json_config.get("cache")
    if cache_config is not None:
        if "dataset_name" in cache_config:
            relative_path = cache_config["path_or_uri"]
            absolute_path = os.path.join(output_path, relative_path)
            cache_config["path_or_uri"] = absolute_path
        else:
            absolute_path = os.path.join(output_path, cache_config["cbm4_config_path"])
            cache_config["cbm4_config_path"] = absolute_path

    for cbmspec_model_config in json_config.get("model_parameters", {}).values():
        increment_table_rel_path = cbmspec_model_config.get("increment_table")
        if increment_table_rel_path:
            increment_table_abs_path = os.path.join(
                os.path.dirname(cbm4_config_path),
                increment_table_rel_path
            )

            cbmspec_model_config["increment_table"] = increment_table_abs_path

    for param_ds_name in ("spinup_parameters", "step_parameters"):
        if param_ds_name not in json_config["cbm4_spatial_dataset"]:
            json_config["cbm4_spatial_dataset"][param_ds_name] = {
                "dataset_name": param_ds_name,
                "storage_type": "local_storage",
                "path_or_uri": os.path.join(output_path, param_ds_name),
            }

    return json_config


def create_cbmspec_models(
    model_parameters: dict[str, Any], cbm_defaults: CBMDefaultsConfig
) -> tuple[CBMSpecModelConfig, CBMSpecModelConfig]:
    spinup_model_config = model_parameters.get("spinup", {})
    step_model_config = model_parameters.get("step", {})
    for model_config in (spinup_model_config, step_model_config):
        model_config["cbm_defaults_path"] = str(cbm_defaults.path)
        model_config["locale"] = cbm_defaults.locale

    spinup_model = CBMSpecModelConfig(spinup_model_config)
    step_model = CBMSpecModelConfig({
        "cbmspec_model": model_parameters.get("cbmspec_model"),
        **step_model_config,
    })

    return spinup_model, step_model


def create_cache(cache_config):
    if "dataset_name" in cache_config:
        # Legacy cache config: simulation dataset config items and cache end year.
        cache_end_year = cache_config["end_year"]
        cache_sim_ds = DatasetConfig(
            cache_config["dataset_name"],
            cache_config["storage_type"],
            cache_config["path_or_uri"],
        )

        cache_param_ds = DatasetConfig(
            "step_parameters",
            Path(cache_config["path_or_uri"]).parent.joinpath("step_parameters"),
            "local_storage",
        )

        return CBMSpecCache(cache_end_year, cache_sim_ds, cache_param_ds)

    # New cache config: path to parent cbm4_config.json and cache end year.
    cache_end_year = cache_config["end_year"]
    cache_config_path = cache_config["cbm4_config_path"]
    cache_config_dir = Path(cache_config_path).parent
    json_config = json.load(open(cache_config_path))
    dataset_config = json_config["cbm4_spatial_dataset"]
    cache_sim_ds = DatasetConfig(
        dataset_config["simulation"]["dataset_name"],
        os.path.join(cache_config_dir, dataset_config["simulation"]["path_or_uri"]),
        dataset_config["simulation"]["storage_type"],
    )

    cache_param_ds = DatasetConfig(
        dataset_config["step_parameters"]["dataset_name"],
        os.path.join(cache_config_dir, dataset_config["step_parameters"]["path_or_uri"]),
        dataset_config["step_parameters"]["storage_type"],
    ) if "step_parameters" in dataset_config else DatasetConfig(
        "step_parameters",
        os.path.join(cache_config_dir, "step_parameters"),
        "local_storage",
    )

    return CBMSpecCache(cache_end_year, cache_sim_ds, cache_param_ds)


def build_run(
    config: dict[str, Any],
    callbacks: CBMSpecCallbacks | None = None,
    progress_bar: tqdm | None = None,
) -> tuple[CBMSpecRun, list[str]]:
    times = []

    dataset_config = config["cbm4_spatial_dataset"]
    inventory_ds = DatasetConfig(**dataset_config["inventory"])
    disturbance_ds = DatasetConfig(**dataset_config["disturbance"])
    simulation_ds = DatasetConfig(**dataset_config["simulation"])

    locale = config.get("cbm_defaults_locale", "en-CA")
    cbm_defaults = CBMDefaultsConfig(inventory_ds, locale)
    model_parameters = config.get("model_parameters", {})
    spinup_model, step_model = create_cbmspec_models(model_parameters, cbm_defaults)

    spinup = None
    cache = create_cache(config["cache"]) if "cache" in config else None
    if cache is None:
        spinup_parameter_ds = DatasetConfig(**dataset_config["spinup_parameters"])
        if not spinup_parameter_ds.exists:
            if progress_bar is not None:
                progress_bar.total += 1
                progress_bar.refresh()

            with timed("create spinup parameter dataset", times, progress_bar):
                spinup_spatial_parameter_ds = (
                    cbm4_parameter_dataset_factory.spinup_parameter_dataset_create(
                        inventory_ds.dataset,
                        disturbance_ds.dataset,
                        **{f"out_{k}": v for k, v in spinup_parameter_ds.constructor_args.items()},
                        enable_cbm_cfs3_smoother=config.get("use_smoother", True),
                    )
                )

                if "increment_table" in spinup_model.config:
                    shutil.rmtree(
                        Path(spinup_parameter_ds.path_or_uri)
                        .joinpath("spinup_parameters-table-increments")
                    )
                    
                    spinup_spatial_parameter_ds.write_table(
                        "parameter_table_metadata",
                        spinup_spatial_parameter_ds.read_table_pandas(
                            "parameter_table_metadata",
                            filters=[("table_name", "!=", "increments")]
                        )
                    )

        spinup = CBMSpecSpinup(
            inventory_ds,
            disturbance_ds,
            simulation_ds,
            spinup_parameter_ds,
            cbm_defaults,
            spinup_model,
            callbacks,
        )

    step_parameter_ds = (
        DatasetConfig(**dataset_config["step_parameters"]) if cache is None
        else cache.step_parameter_ds
    )

    if not step_parameter_ds.exists:
        with timed("create step parameter datasets", times, progress_bar):
            step_spatial_parameter_ds = (
                cbm4_parameter_dataset_factory.step_parameter_dataset_create(
                    inventory_ds.dataset,
                    disturbance_ds.dataset,
                    **{f"out_{k}": v for k, v in step_parameter_ds.constructor_args.items()},
                    enable_cbm_cfs3_smoother=config.get("use_smoother", True),
                )
            )

            if "increment_table" in step_model.config:
                shutil.rmtree(
                    Path(step_parameter_ds.path_or_uri)
                    .joinpath("step_parameters-table-increments")
                )
                
                step_spatial_parameter_ds.write_table(
                    "parameter_table_metadata",
                    step_spatial_parameter_ds.read_table_pandas(
                        "parameter_table_metadata",
                        filters=[("table_name", "!=", "increments")]
                    )
                )

    stepping = CBMSpecStepping(
        inventory_ds,
        disturbance_ds,
        simulation_ds,
        step_parameter_ds,
        cbm_defaults,
        config["start_year"],
        config["end_year"],
        step_model,
        cache,
        callbacks,
    )

    return CBMSpecRun(spinup, stepping), times
