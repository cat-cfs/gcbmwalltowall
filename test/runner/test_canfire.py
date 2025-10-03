import json
import shutil
from pathlib import Path
from typing import Any

from cbm4_canfire.cbmspec.canfire_cbmspec_model import CanfireCbmSpecModel
from cbm4_canfire.cbmspec.canfire_config import CanfireConfig
from cbm4_canfire.cbmspec.logging_config import LoggingConfig
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix

from gcbmwalltowall.runner.canfire import load_config, run

standalone_project_path = Path(__file__).parent.joinpath("resources", "standalone")
results_dir = Path(__file__).parent.joinpath("run_results")

standalone_canfire_config = CanfireConfig(
    species_map={
        "JP": "PINUBAN",
        "TA": "POPUTRE",
        "BF": "ABIEBAL",
        "BP": "POPUBAL",
        "GA": "ABIEGRA",
        "WB": "BETUPAP",
        "BS": "PICEMAR",
        "WS": "PICEGLA",
    },
    column_mapping={
        "isi": "initial_spread_index",
        "bui": "buildup_index",
        "ffmc": "fine_fuel_moisture_code",
        "dc": "drought_code",
        "FuelBed": "fuel_bed_type",
        "spatial_unit_id": "ru",
        "Classifier1": "Species",
    },
    default_values={
        "julian_day": 196,
        "lat": 1,
        "lon": 1,
        "elevation": 1,
        "buildup_index": 1,
        "initial_spread_index": 1,
        "fine_fuel_moisture_code": 1,
        "drought_code": 1,
        "fuel_bed_type": "C2",
        "hard_wood_species": "",
        "soft_wood_species": "",
        "ru": 21,
    },
    logging_config=LoggingConfig.model_validate(
        {
            "input_stand_log": str(
                results_dir.joinpath("test_run_canfire", "canfire_logs")
            ),
            "disturbance_matrix_log": str(
                results_dir.joinpath("test_run_canfire", "canfire_logs")
            ),
        }
    ),
)


def test_load_config():
    model = cbmspec_cbm3_single_matrix.model_create()

    json_config = load_config(
        str(standalone_project_path.joinpath("cbm4_config.json")),
        wrapped_cbmspec_model=model,
    )

    assert isinstance(json_config.get("cbmspec_model"), CanfireCbmSpecModel)


def test_run_canfire():
    dst = results_dir.joinpath("test_run_canfire")
    if dst.exists():
        shutil.rmtree(dst)

    project = Path(shutil.copytree(standalone_project_path, dst))

    config: dict[str, Any] = json.load(project.joinpath("cbm4_config.json").open())
    config.update({"modules": {"canfire": standalone_canfire_config.model_dump()}})
    json.dump(config, project.joinpath("cbm4_config.json").open("w"), indent=4)

    cbm4_config_path = str(project.joinpath("cbm4_config.json"))
    model = cbmspec_cbm3_single_matrix.model_create()

    run(
        cbm4_config_path,
        wrapped_cbmspec_model=model,
    )
