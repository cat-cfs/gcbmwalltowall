import json
from typing import Any
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from cbm4_canfire.cbmspec.canfire_cbmspec_model import CanfireCbmSpecModel
from cbm4_canfire.cbmspec.canfire_config import CanfireConfig
from cbm4_canfire.cbmspec.cbmspec import CBMSpecModel
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.runner import cbmspec
from gcbmwalltowall.runner.cbmspec import CBMDefaultsRef


def get_single_matrix_cbmspec(cbm4_config_path: str, **config: Any):
    json_config = cbmspec.load_config(cbm4_config_path, **config)
    inventory_ds = RasterIndexedDataset(
        json_config["cbm4_spatial_dataset"]["inventory"]["dataset_name"],
        json_config["cbm4_spatial_dataset"]["inventory"]["storage_type"],
        json_config["cbm4_spatial_dataset"]["inventory"]["path_or_uri"],
    )

    cbm_defaults = CBMDefaultsRef(inventory_ds)
    increment_table = json_config.get("model_parameters", {}).get("step", {}).get("increment_table")
    cbmspec_cbm3_single_matrix_model = cbmspec_cbm3_single_matrix.model_create(
        str(cbm_defaults.path),
        json_config.get("cbm_defaults_locale", "en-CA"),
        increment_table
    )

    return cbmspec_cbm3_single_matrix_model


def load_config(
    cbm4_config_path: str,
    wrapped_cbmspec_model: CBMSpecModel,
    **kwargs: Any,
) -> dict[str, Any]:
    canfire_config_source: dict[str, Any] | str = (
        json.load(Path(cbm4_config_path).open("rb"))
        .get("modules", dict())
        .get("canfire", dict())
    )

    if isinstance(canfire_config_source, str):
        canfire_config = CanfireConfig.model_validate_json(
            Path(canfire_config_source).open("r").read()
        )
    else:
        canfire_config = CanfireConfig.model_validate(canfire_config_source)

    model = CanfireCbmSpecModel(
        wrapped_cbmspec_model,
        canfire_config,
    )

    json_config = cbmspec.load_config(cbm4_config_path, **kwargs)  # type: ignore
    json_config.update({"cbmspec_model": model})

    return json_config


def run(cbm4_config_path: str, wrapped_cbmspec_model: CBMSpecModel, **kwargs: Any):
    kwargs["json_config"] = load_config(
        cbm4_config_path, wrapped_cbmspec_model, **kwargs
    )

    return cbmspec.run(cbm4_config_path, **kwargs)  # type: ignore
