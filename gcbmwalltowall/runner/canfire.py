from pathlib import Path
from typing import Any

from cbm4_canfire.cbmspec.canfire_cbmspec_model import CanfireCbmSpecModel
from cbm4_canfire.cbmspec.canfire_config import CanfireConfig
from cbm4_canfire.cbmspec.cbmspec import CBMSpecModel

from . import cbmspec


def load_config(
    cbm4_config_path: str,
    wrapped_cbmspec_model: CBMSpecModel,
    **kwargs: Any,
) -> dict[str, Any]:
    # This line could change to something else if the cbm4_config changes
    # currently assuming that it would be in a flat shape
    canfire_config = CanfireConfig.model_validate_json(
        Path(cbm4_config_path).open("rb").read()
    )

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
