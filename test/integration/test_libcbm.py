import json
import pandas as pd
from tempfile import TemporaryDirectory
from pathlib import Path
from gcbmwalltowall.application.walltowall import extend
from gcbmwalltowall.runner.cbm4 import run
from arrow_space.raster_indexed_dataset import RasterIndexedDataset


def test_full_workflow_libcbm_extended_with_new_yields(cbm4_project_copy):
    cbm4_config_path = str(cbm4_project_copy.joinpath("cbm4_config.json"))
    cbm4_config = json.load(open(cbm4_config_path))
    run(cbm4_config_path)

    with TemporaryDirectory() as tmp:
        event_data = pd.DataFrame({
            "event_id": ["1"],
            "disturbance_type": "Natural succession",
            "target_type": "area",
            "target": 1000,
            "selector_type": "random",
            "selector": "",
            "filter_type": "dnf",
            "filter": "",
            "year": 2015,
            "transition": '{"classifiers.Classifier1": "X"}'
        })

        event_data.to_csv(Path(tmp).joinpath("events.csv"), index=False)
        json.dump(
            {"disturbances": {"events": "events.csv"}},
            open(Path(tmp).joinpath("events.json"), "w")
        )

        extend({
            "cbm4_config_path": str(cbm4_project_copy.joinpath("cbm4_config.json")),
            "disturbance_config_path": str(Path(tmp).joinpath("events.json")),
            "output_path": str(Path(tmp).joinpath("cbm4_extended")),
        })

        def extend_yields(sim_ds_path):
            ds = RasterIndexedDataset("simulation", "local_storage", sim_ds_path)
            yields = ds.read_table_pandas("yield")
            new_yields = yields.copy()
            new_yields["Classifier1"] = "X"
            ds.write_table("yield", pd.concat((yields, new_yields)))

        run(
            str(Path(tmp).joinpath("cbm4_extended/cbm4_config.json")),
            on_pre_simulation=extend_yields,
        )

        extended_run_sim_ds = RasterIndexedDataset(
            "simulation",
            "local_storage",
            str(Path(tmp).joinpath("cbm4_extended/simulation"))
        )

        extended_run_timesteps = {p["timestep"] for p in extended_run_sim_ds.get_partition_values()}
        expected_first_addon_timestep = 2015 - (cbm4_config["start_year"] - 1)
        expected_last_addon_timestep = cbm4_config["end_year"] - (cbm4_config["start_year"] - 1)
        assert extended_run_timesteps == set(
            range(expected_first_addon_timestep, expected_last_addon_timestep + 1)
        )

        original_sim_ds = RasterIndexedDataset(
            cbm4_config["cbm4_spatial_dataset"]["simulation"]["dataset_name"],
            cbm4_config["cbm4_spatial_dataset"]["simulation"]["storage_type"],
            str(cbm4_project_copy.joinpath(cbm4_config["cbm4_spatial_dataset"]["simulation"]["path_or_uri"])),
        )

        assert "X" not in original_sim_ds.read_table_pandas("yield")["Classifier1"].to_list()
        assert "X" in extended_run_sim_ds.read_table_pandas("yield")["Classifier1"].to_list()
