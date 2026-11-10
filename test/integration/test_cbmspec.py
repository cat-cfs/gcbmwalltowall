from pathlib import Path
from gcbmwalltowall.application.walltowall import prepare
from gcbmwalltowall.application.walltowall import convert
from gcbmwalltowall.application.walltowall import run
from arrow_space.raster_indexed_dataset import RasterIndexedDataset


def test_full_workflow_cbmspec_spatial_parameters():
    test_project_root = Path(__file__).parent.parent.joinpath(
        "resources/cbmspec_spatial_parameters"
    )

    prepare({
        "config_path": str(test_project_root.joinpath("config/walltowall_config.json")),
        "output_path": str(test_project_root),
    })

    convert({
        "project_path": str(test_project_root),
        "output_path": str(test_project_root.joinpath("cbm4")),
    })

    run({
        "project_path": str(test_project_root.joinpath("cbm4")),
        "engine": "cbmspec",
        "write_parameters": True,
    })

    # Test run uses a mean_annual_temperature timeseries covering the whole
    # landscape, where 2010 = 10, 2011 = 11, 2012 = 12, 2015=15. Check that these
    # values were used, and that they were gap-filled and extended backward and
    # forward to cover all simulation timesteps:
    sim_ds = RasterIndexedDataset(
        "simulation",
        "local_storage",
        str(test_project_root.joinpath("cbm4/simulation"))
    )

    sim_parameter_data = sim_ds.read_table_pandas("step_parameters")
    mat_values = sim_parameter_data[["timestep", "mean_annual_temperature"]].drop_duplicates()
    assert len(mat_values) == 21
    for timestep, expected_mat in (
        (1, 10), (11, 10), (12, 11), (13, 12), (14, 12), (15, 12), (16, 15), (21, 15)
    ):
        assert (
            mat_values[mat_values.timestep == timestep]["mean_annual_temperature"].item()
            == expected_mat
        )
