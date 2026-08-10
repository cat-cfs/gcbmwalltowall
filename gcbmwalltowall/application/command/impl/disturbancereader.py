import json
import pandas as pd
from pathlib import Path
from typing import Literal
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from arrow_space.raster_indexed_dataset import RasterIndexedDataset
from libcbm.model.cbm.cbm_defaults_reference import CBMDefaultsReference
from cbm4.app.spatial.gcbm_input.timestep_interpreter import YearOffsetTimestepInterpreter
from gcbmwalltowall.application.command.impl.disturbanceformatter import DisturbanceFormatter
from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project


class DisturbanceReader:

    def __init__(
        self,
        walltowall_disturbance_dataset: FlattenedCoordinateDataset,
        disturbance_formatter: DisturbanceFormatter,
        transition_offset: int = 0,
    ):
        self._dataset = walltowall_disturbance_dataset
        self._disturbance_formatter = disturbance_formatter
        self._transition_offset = transition_offset

    @classmethod
    def starting_from(
        cls,
        cbm4_project: CBM4Project,
        walltowall_disturbance_dataset: FlattenedCoordinateDataset,
    ):
        transition_offset = cbm4_project.get_max_transition_id()
        cbm_defaults_ref = CBMDefaultsReference(str(cbm4_project.cbm_defaults_path))
        timestep_interpreter = YearOffsetTimestepInterpreter(cbm4_project.t0_year)
        disturbance_formatter = DisturbanceFormatter(cbm_defaults_ref, timestep_interpreter)

        return cls(
            walltowall_disturbance_dataset,
            disturbance_formatter,
            transition_offset,
        )

    def read_disturbances(self, filters: list | None = None) -> pd.DataFrame | None:
        disturbance_data = None
        for layer_name in self._dataset.get_layer_names():
            layer_data = self._read_disturbance_layer(layer_name, filters)
            if layer_data is None or layer_data.empty:
                continue

            if disturbance_data is None:
                disturbance_data = layer_data
            else:
                disturbance_data = pd.concat((disturbance_data, layer_data))

        if disturbance_data is not None:
            for col in ("undisturbed_transition_id", "disturbed_transition_id"):
                disturbance_data.loc[
                    disturbance_data[col] != -1, col
                ] += self._transition_offset

        return disturbance_data

    def read_transitions(
        self,
    ) -> dict[
        Literal["transitions_disturbed", "transitions_undisturbed"], pd.DataFrame
    ]:
        transitions = {}
        for transition_table in ("transitions_disturbed", "transitions_undisturbed"):
            if not self._dataset.table_exists(transition_table):
                continue

            transition_data = self._dataset.read_table_pandas(transition_table)
            transition_data["id"] += self._transition_offset
            transitions[transition_table] = transition_data

        return transitions

    def _read_disturbance_layer(
        self,
        layer_name: str,
        filters: list | None = None,
    ) -> pd.DataFrame | None:
        disturbances = self._dataset.read_pandas(
            read_cols=[layer_name], filters=filters
        )

        if disturbances is None or disturbances.empty:
            return

        disturbance_attrs = self._dataset.get_attributes(layer_name)
        assert disturbance_attrs is not None
        disturbance_data = pd.merge(
            disturbances, disturbance_attrs, left_on=layer_name, right_on="id"
        ).dropna(
            subset=["year", "disturbance_type"]
        ).rename(columns={"transition": "disturbed_transition_id"})

        self._disturbance_formatter.format(disturbance_data)

        return disturbance_data[
            [col for col in disturbance_data.columns if col not in ("id", layer_name)]
        ]
