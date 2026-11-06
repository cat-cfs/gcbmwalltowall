from __future__ import annotations
import pandas as pd
import pathlib
from tqdm import tqdm
from typing import Callable
from tqdm import tqdm
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecCallbacks
from gcbmwalltowall.runner.helpers.cbmspec_config import build_run
from gcbmwalltowall.runner.helpers.cbmspec_config import load_config
from gcbmwalltowall.util.path import Path


def run(
    cbm4_config_path: str | pathlib.Path,
    max_workers: int | None = None,
    write_parameters: bool = False,
    on_pre_spinup: Callable[[str]] | None = None,
    on_pre_simulation: Callable[[str]] | None = None,
    end_year: int | None = None,
    **kwargs,
):
    json_config = kwargs.get("json_config") or load_config(cbm4_config_path, **kwargs)
    json_config["end_year"] = end_year or json_config["end_year"]
    callbacks = CBMSpecCallbacks(
        legacy_on_pre_spinup=on_pre_spinup,
        legacy_on_pre_simulation=on_pre_simulation
    )

    with tqdm(
        desc="initializing",
        total=0,
        bar_format=r"[Running simulation] {l_bar}{bar}{r_bar}"
    ) as progress_bar:
        cbmspec_run, times = build_run(json_config, callbacks)
        times.extend(
            cbmspec_run.run(
                progress_bar,
                max_workers=max_workers,
                write_parameters=write_parameters,
                allow_multiple_transitions=json_config.get(
                    "allow_multiple_transitions", False
                ),
            )
        )

        time_profiling = pd.DataFrame(columns=["task", "time_elapsed"], data=times)
        time_profiling.to_csv(
            Path(cbm4_config_path).parent.joinpath("profiling.csv"),
            index=False
        )
