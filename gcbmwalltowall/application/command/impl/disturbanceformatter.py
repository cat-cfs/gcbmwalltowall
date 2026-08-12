import pandas as pd
from libcbm.model.cbm.cbm_defaults_reference import CBMDefaultsReference
from cbm4.app.spatial.gcbm_input.timestep_interpreter import TimestepInterpreter


class DisturbanceFormatter:

    def __init__(
        self,
        cbm_defaults: CBMDefaultsReference,
        timestep_interpretation: TimestepInterpreter,
    ):
        self._timestep_interpretation = timestep_interpretation
        self._disturbance_type_map = {
            str(row["disturbance_type_name"]): int(row["disturbance_type_id"])
            for row in cbm_defaults.disturbance_type_ref
        }

    def format(self, disturbance_events: pd.DataFrame):
        disturbance_events.dropna(subset=["year", "disturbance_type"], inplace=True)
        disturbance_events.rename(
            columns={"transition": "disturbed_transition_id"}, inplace=True
        )

        for attr, default in {
            "disturbance_id": -1,
            "disturbance_order": -1,
            "proportion": 1.0,
            "enable_merge": False,
            "sort_id": 0,
            "filter_id": 0,
            "undisturbed_transition_id": 0,
            "disturbed_transition_id": 0,
        }.items():
            if attr not in disturbance_events:
                disturbance_events[attr] = default

        disturbance_events["timestep"] = disturbance_events.apply(
            self._timestep_interpretation.get_timestep, axis=1
        )

        disturbance_events["default_disturbance_type_id"] = disturbance_events.apply(
            lambda event: self._disturbance_type_map[event["disturbance_type"]], axis=1
        )
