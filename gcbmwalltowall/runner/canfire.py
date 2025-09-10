import json
from pathlib import Path
from typing import Any

from cbm4_canfire.cbmspec.canfire_cbmspec_model import CanfireCbmSpecModel
from cbm4_canfire.cbmspec.canfire_config import CanfireConfig
from cbm4_canfire.cbmspec.cbmspec import CBMSpecModel

from . import cbmspec


def load_config(
    cbm4_config_path: str,
    **kwargs: Any,
) -> dict[str, Any]:
    canfire_config = CanfireConfig().model_validate_json(
        json.load(Path(cbm4_config_path).open())
    )

    wrapped_model = kwargs.get("cbmspec_model")

    if not isinstance(wrapped_model, CBMSpecModel):
        raise ValueError("needs cbmspec model")

    model = CanfireCbmSpecModel(
        wrapped_model,
        canfire_config,
    )

    kwargs["update_fields"]["cbmspec_model"] = model

    return cbmspec.load_config(cbm4_config_path, **kwargs)  # type: ignore


def run(cbm4_config_path: str, **kwargs: Any):
    update_fields_dict: dict[str, Any] = dict()
    json_config = load_config(
        cbm4_config_path, update_fields=update_fields_dict, **kwargs
    )
    kwargs.update(update_fields_dict)
    kwargs["json_config"] = json_config
    return cbmspec.run(cbm4_config_path, **kwargs)  # type: ignore
