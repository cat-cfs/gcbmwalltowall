import gcbmwalltowall
import shutil
from typing import Iterator
from pytest import fixture
from tempfile import TemporaryDirectory
from pathlib import Path
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from pathlib import Path
from gcbmwalltowall.application.walltowall import prepare
from gcbmwalltowall.application.walltowall import convert
from gcbmwalltowall.application.walltowall import run
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer


@fixture(scope="module")
def cbm4_input_path() -> Path:
    return Path(gcbmwalltowall.__file__).parent.parent.joinpath(
        "test", "resources", "historic_yields"
    )


@fixture
def cbm4_project_copy(cbm4_input_path: Path) -> Iterator[Path]:
    with TemporaryDirectory() as tmp:
        yield Path(shutil.copytree(cbm4_input_path, tmp, dirs_exist_ok=True))


def test_libcbm_historic_yields(cbm4_project_copy):
    walltowall_config_path = str(cbm4_project_copy.joinpath("config/walltowall_config.json"))

    prepare({
        "config_path": str(cbm4_project_copy.joinpath("config/walltowall_config.json")),
        "output_path": str(cbm4_project_copy),
    })

    convert({
        "project_path": str(cbm4_project_copy),
        "output_path": str(cbm4_project_copy.joinpath("cbm4")),
    })

    run({"project_path": str(cbm4_project_copy.joinpath("cbm4"))})

    sim_ds = RasterIndexedDataset(
        "simulation",
        "local_storage",
        str(cbm4_project_copy.joinpath("cbm4/simulation"))
    )

    bg_slow_historic_yields = sim_ds.read_pandas(
        filters=[["timestep", "=", 0]], read_cols=["pools.BelowGroundSlowSoil"]
    ).sum()

    del sim_ds

    with GCBMConfigurer.update_json_file(walltowall_config_path) as walltowall_config:
        del walltowall_config["historic_yield_table"]

    prepare({
        "config_path": str(cbm4_project_copy.joinpath("config/walltowall_config.json")),
        "output_path": str(cbm4_project_copy),
    })

    convert({
        "project_path": str(cbm4_project_copy),
        "output_path": str(cbm4_project_copy.joinpath("cbm4")),
    })

    run({"project_path": str(cbm4_project_copy.joinpath("cbm4"))})

    sim_ds = RasterIndexedDataset(
        "simulation",
        "local_storage",
        str(cbm4_project_copy.joinpath("cbm4/simulation"))
    )

    bg_slow_base_yields = sim_ds.read_pandas(
        filters=[["timestep", "=", 0]],
        read_cols=["pools.BelowGroundSlowSoil"],
        include_index_cols=False,
    ).sum()

    assert (
        bg_slow_historic_yields["pools.BelowGroundSlowSoil"]
        > bg_slow_base_yields["pools.BelowGroundSlowSoil"]
    )


def test_cbmspec_historic_yields(cbm4_project_copy):
    walltowall_config_path = str(cbm4_project_copy.joinpath("config/walltowall_config.json"))

    prepare({
        "config_path": str(cbm4_project_copy.joinpath("config/walltowall_config.json")),
        "output_path": str(cbm4_project_copy),
    })

    convert({
        "project_path": str(cbm4_project_copy),
        "output_path": str(cbm4_project_copy.joinpath("cbm4")),
        "optimize_spinup": True,
    })

    run({
        "project_path": str(cbm4_project_copy.joinpath("cbm4")),
        "engine": "cbmspec",
    })

    sim_ds = RasterIndexedDataset(
        "simulation",
        "local_storage",
        str(cbm4_project_copy.joinpath("cbm4/simulation"))
    )

    bg_slow_historic_yields = sim_ds.read_pandas(
        filters=[["timestep", "=", 0]], read_cols=["pools.BelowGroundSlowSoil"]
    ).sum()

    del sim_ds

    with GCBMConfigurer.update_json_file(walltowall_config_path) as walltowall_config:
        del walltowall_config["historic_yield_table"]

    prepare({
        "config_path": str(cbm4_project_copy.joinpath("config/walltowall_config.json")),
        "output_path": str(cbm4_project_copy),
    })

    convert({
        "project_path": str(cbm4_project_copy),
        "output_path": str(cbm4_project_copy.joinpath("cbm4")),
        "optimize_spinup": True,
    })

    run({
        "project_path": str(cbm4_project_copy.joinpath("cbm4")),
        "engine": "cbmspec",
    })

    sim_ds = RasterIndexedDataset(
        "simulation",
        "local_storage",
        str(cbm4_project_copy.joinpath("cbm4/simulation"))
    )

    bg_slow_base_yields = sim_ds.read_pandas(
        filters=[["timestep", "=", 0]],
        read_cols=["pools.BelowGroundSlowSoil"],
        include_index_cols=False,
    ).sum()

    assert (
        bg_slow_historic_yields["pools.BelowGroundSlowSoil"]
        > bg_slow_base_yields["pools.BelowGroundSlowSoil"]
    )
