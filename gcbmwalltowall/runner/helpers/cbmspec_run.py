from __future__ import annotations
import time
import pathlib
from tqdm import tqdm
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from cbm4.app.spatial.spatial_cbm4 import cbm4_spatial_runner
from cbm4.app.spatial.event_handler.event_processor import EventProcessor
from tqdm import tqdm
from gcbmwalltowall.converter.parameterdatasetpreprocessor import ParameterDatasetPreprocessor
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMDefaultsConfig
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecCache
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecCallbacks
from gcbmwalltowall.runner.helpers.cbmspec_components import CBMSpecModelConfig
from gcbmwalltowall.runner.helpers.cbmspec_components import DatasetConfig
from gcbmwalltowall.util.path import Path
from cbm4.app.spatial.gcbm_input.timestep_interpreter import YearOffsetTimestepInterpreter


@contextmanager
def timed(
    label: str,
    out_messages: list[tuple[str, float]],
    progress_bar: tqdm | None = None
):
    start = time.time()
    yield
    out_messages.append((label, (time.time() - start)))
    if progress_bar is not None:
        progress_bar.update()


@dataclass
class CBMSpecSpinup:
    inventory_ds: DatasetConfig
    disturbance_ds: DatasetConfig
    simulation_ds: DatasetConfig
    parameter_ds: DatasetConfig
    cbm_defaults: CBMDefaultsConfig
    cbmspec_model: CBMSpecModelConfig
    callbacks: CBMSpecCallbacks | None = None

    def run(
        self,
        progress_bar: tqdm | None = None,
        **spinup_kwargs
    ) -> list[str]:
        times = []
        callbacks = self.callbacks or CBMSpecCallbacks()
        inventory = self.inventory_ds.dataset
        parameters = self.parameter_ds.dataset
        model = self.cbmspec_model.model

        if progress_bar:
            progress_bar.total += sum((
                2,
                (
                    callbacks.legacy_on_pre_spinup is not None
                    or callbacks.on_pre_spinup is not None
                ),
            ))

            progress_bar.refresh()

        simulation = None
        with timed("create_simulation_dataset", times, progress_bar):
            if self.simulation_ds.exists:
                self.simulation_ds.dataset.delete()

            simulation = cbm4_spatial_runner.create_simulation_dataset(
                model,
                inventory,
                **self.simulation_ds.to_constructor_args("out"),
            )

        if callbacks.legacy_on_pre_spinup is not None:
            with timed("pre-spinup callback", times, progress_bar):
                callbacks.legacy_on_pre_spinup(str(self.simulation_ds.path_or_uri))
        elif callbacks.on_pre_spinup is not None:
            with timed("pre-spinup callback", times, progress_bar):
                callbacks.on_pre_spinup(self)

        with timed("spinup", times, progress_bar):
            cbm4_spatial_runner.spinup_all(
                model=model,
                inventory_dataset=inventory,
                simulation_dataset=simulation,
                parameter_dataset=parameters,
                max_workers=spinup_kwargs.get("max_workers"),
                write_parameters=spinup_kwargs.get("write_parameters", False),
            )

        return times


@dataclass
class CBMSpecStepping:
    inventory_ds: DatasetConfig
    disturbance_ds: DatasetConfig
    simulation_ds: DatasetConfig
    parameter_ds: DatasetConfig
    cbm_defaults: CBMDefaultsConfig
    start_year: int
    end_year: int
    cbmspec_model: CBMSpecModelConfig
    cache: CBMSpecCache | None = None
    callbacks: CBMSpecCallbacks | None = None

    def run(
        self,
        progress_bar: tqdm | None = None,
        temp_dir: str | pathlib.Path | None = None,
        **step_kwargs
    ) -> list[str]:
        times = []
        callbacks = self.callbacks or CBMSpecCallbacks()
        model = self.cbmspec_model.model

        start_timestep = (
            1 if self.cache is None
            else (self.cache.end_year - (self.start_year - 1) + 1)
        )
        
        end_timestep = self.end_year - (self.start_year - 1)
        timesteps = list(range(start_timestep, end_timestep + 1))

        if progress_bar:
            progress_bar.total += sum((
                1,
                len(timesteps),
                self.cache is not None,
                (
                    callbacks.legacy_on_pre_simulation is not None
                    or callbacks.on_pre_simulation is not None
                ),
            ))

            progress_bar.refresh()

        if temp_dir:
            temp_dir = Path(temp_dir)
        else:
            _temp_dir = TemporaryDirectory()
            temp_dir = Path(_temp_dir.name)

        if self.cache is not None:
            with timed("create simulation dataset", times, progress_bar):
                if self.simulation_ds.exists:
                    self.simulation_ds.dataset.delete()

                cbm4_spatial_runner.create_simulation_dataset(
                    model,
                    self.inventory_ds.dataset,
                    **self.simulation_ds.to_constructor_args("out"),
                )

        # Create working copies of the disturbance and parameter datasets to
        # preserve the integrity of the originals and make re-runs safe.
        with timed("create working datasets", times, progress_bar):
            working_disturbance_ds = self.disturbance_ds.copy(temp_dir)
            working_parameter_ds = self.parameter_ds.copy(temp_dir)
            ParameterDatasetPreprocessor(
                YearOffsetTimestepInterpreter(self.start_year - 1),
                str(self.cbm_defaults.path),
            ).ensure_timeseries_coverage(
                working_parameter_ds.dataset, self.start_year, self.end_year
            )

        callback_datasets = CBMSpecStepping(
            self.inventory_ds,
            working_disturbance_ds,
            self.simulation_ds,
            working_parameter_ds,
            self.cbm_defaults,
            self.start_year,
            self.end_year,
            self.cbmspec_model,
            self.cache,
        )

        if callbacks.legacy_on_pre_simulation is not None:
            with timed("pre-simulation callback", times, progress_bar):
                callbacks.legacy_on_pre_simulation(str(self.simulation_ds.path_or_uri))
        elif callbacks.on_pre_simulation is not None:
            with timed("pre-simulation callback", times, progress_bar):
                callbacks.on_pre_simulation(callback_datasets)

        simulation = self.simulation_ds.dataset
        disturbance = working_disturbance_ds.dataset
        parameters = working_parameter_ds.dataset

        t0_event_processor = None
        event_processor = EventProcessor.for_datasets(
            simulation,
            disturbance,
            cbmspec_model_config={
                "package_name": "cbmspec",
                "factory_function": "cbmspec_cbm3.models.cbmspec_cbm3_single_matrix.model_create",
                "factory_parameters": self.cbmspec_model.config,
            }
        )

        for i, timestep in enumerate(timesteps):
            cache_sim_ds = (
                self.cache.simulation_output_ds.dataset
                if self.cache is not None else None
            )

            if cache_sim_ds is not None and i == 0:
                t0_event_processor = EventProcessor.for_datasets(
                    cache_sim_ds,
                    disturbance,
                    cbmspec_model_config={
                        "package_name": "cbmspec",
                        "factory_function": "cbmspec_cbm3.models.cbmspec_cbm3_single_matrix.model_create",
                        "factory_parameters": self.cbmspec_model.config,
                    }
                )
                
                t0_event_processor.process_events_for_timestep(timestep)
            else:
                event_processor.process_events_for_timestep(timestep)

            with timed(f"timestep_{timestep}", times, progress_bar):
                cbm4_spatial_runner.step_all(
                    model=model,
                    timestep=timestep,
                    simulation_input_dataset=(
                        cache_sim_ds if cache_sim_ds is not None and i == 0
                        else simulation
                    ),
                    disturbance_event_dataset=disturbance,
                    simulation_output_dataset=simulation,
                    parameter_dataset=parameters,
                    max_workers=step_kwargs.get("max_workers"),
                    write_parameters=step_kwargs.get("write_parameters", False),
                    allow_multiple_transitions=step_kwargs.get("allow_multiple_transitions", False),
                )

        summary_out_path = Path(self.simulation_ds.path_or_uri).parent
        if t0_event_processor is not None:
            t0_event_processor.summarize(
                summary_out_path.joinpath("event_processor_summary_t0.csv")
            )

        event_processor.summarize(summary_out_path.joinpath("event_processor_summary.csv"))

        return times


@dataclass
class CBMSpecRun:
    spinup: CBMSpecSpinup | None = None
    step: CBMSpecStepping | None = None

    def run(self, progress_bar: tqdm | None = None, **run_kwargs) -> list[str]:
        step_times = []
        if progress_bar is not None:
            progress_bar.total += sum((
                self.spinup is not None,
                self.step is not None,
            ))

            progress_bar.refresh()

        if self.spinup:
            step_times.extend(self.spinup.run(progress_bar, **run_kwargs))
            if progress_bar is not None:
                progress_bar.update()
        
        if self.step:
            step_times.extend(self.step.run(progress_bar, **run_kwargs))
            if progress_bar is not None:
                progress_bar.update()

        return step_times
