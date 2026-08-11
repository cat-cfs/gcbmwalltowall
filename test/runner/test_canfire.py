import json
from pytest import fixture
from cbm4_canfire.cbmspec.canfire_cbmspec_model import CanfireCbmSpecModel
from cbm4_canfire.cbmspec.canfire_config import CanfireConfig
from cbm4_canfire.cbmspec.logging_config import LoggingConfig
from cbmspec_cbm3.models import cbmspec_cbm3_single_matrix
from gcbmwalltowall.runner.canfire import load_config, run


@fixture
def canfire_config(cbm4_project_copy):
    return CanfireConfig(
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
            "ru": "spatial_unit",
            "Species": "Classifier1",
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
            "Species": "",
            "SoftwoodSpecies": "",
            "HardwoodSpecies": "",
        },
        logging_config=LoggingConfig.model_validate(
            {
                "input_stand_log": str(
                    cbm4_project_copy.joinpath("test_run_canfire", "canfire_logs")
                ),
                "disturbance_matrix_log": str(
                    cbm4_project_copy.joinpath("test_run_canfire", "canfire_logs")
                ),
            }
        ),
    )


def test_load_config(cbm4_config_path):
    model = cbmspec_cbm3_single_matrix.model_create()
    json_config = load_config(str(cbm4_config_path), model)
    assert isinstance(json_config.get("cbmspec_model"), CanfireCbmSpecModel)


def test_run_canfire(cbm4_project_copy, canfire_config):
    cbm4_config_path = cbm4_project_copy.joinpath("cbm4_config.json")
    config = json.load(cbm4_config_path.open())
    config.update({"modules": {"canfire": canfire_config.model_dump()}})
    json.dump(config, cbm4_config_path.open("w"), indent=4)
    model = cbmspec_cbm3_single_matrix.model_create()
    run(cbm4_config_path, model)
