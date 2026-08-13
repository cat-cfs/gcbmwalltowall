import gcbmwalltowall
import polars as pl
from pytest import fixture
from pathlib import Path
from gcbmwalltowall.application.command.impl.disturbanceextender import DisturbanceExtender
from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project


@fixture
def extra_tiled_disturbance_path() -> Path:
    return Path(gcbmwalltowall.__file__).parent.parent.joinpath(
        "test", "resources", "extra_disturbances"
    )


@fixture
def extra_walltowall_disturbance_path() -> Path:
    return Path(gcbmwalltowall.__file__).parent.parent.joinpath(
        "test", "resources", "extra_walltowall_disturbances"
    )


def test_add_from_study_area(cbm4_project_copy, extra_tiled_disturbance_path):
    cbm4_project = CBM4Project(cbm4_project_copy.joinpath("cbm4_config.json"))
    extender = DisturbanceExtender(cbm4_project)
    study_area_path = extra_tiled_disturbance_path.joinpath("study_area.json")

    original_disturbance_count = (
        cbm4_project.disturbance_dataset.read_polars()
        .select(pl.len()).collect().item()
    )

    extender.add_from_study_area(study_area_path)
    extended_disturbance_count = (
        cbm4_project.disturbance_dataset.read_polars()
        .select(pl.len()).collect().item()
    )

    assert extended_disturbance_count > original_disturbance_count


def test_add_from_walltowall_config(
    cbm4_project_copy, extra_walltowall_disturbance_path
):
    cbm4_project = CBM4Project(cbm4_project_copy.joinpath("cbm4_config.json"))
    extender = DisturbanceExtender(cbm4_project)
    walltowall_config_path = extra_walltowall_disturbance_path.joinpath(
        "walltowall_config.json"
    )

    original_disturbance_count = (
        cbm4_project.disturbance_dataset.read_polars()
        .select(pl.len()).collect().item()
    )

    extender.add_from_walltowall_config(walltowall_config_path)
    extended_disturbance_count = (
        cbm4_project.disturbance_dataset.read_polars()
        .select(pl.len()).collect().item()
    )

    assert extended_disturbance_count > original_disturbance_count
