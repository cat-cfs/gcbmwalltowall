import json
import numpy as np
import pandas as pd
from pathlib import Path
from gcbmwalltowall.util.encoding import load_csv


class RuleBasedEventConverter:

    def __init__(self, next_transition_id: int = 1):
        self._next_transition_id = int(next_transition_id)

    def convert(
        self,
        rule_based_disturbances_path: str | Path,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        events = load_csv(rule_based_disturbances_path)
        if "transition" not in events:
            return events, None

        events.loc[events["transition"].isna(), "disturbed_transition_id"] = -1
        events.loc[~events["transition"].isna(), "disturbed_transition_id"] = (
            np.arange(len(events[~events["transition"].isna()]))
            + self._next_transition_id
        )

        transitions = pd.DataFrame(
            {"id": row["disturbed_transition_id"], **json.loads(row["transition"])}
            for _, row in
            events.loc[
                ~events["transition"].isna(),
                ["disturbed_transition_id", "transition"]
            ].iterrows()
        )

        required_cols = {
            ("id", "int"): -1,
            ("state.regeneration_delay", "int"): 0,
            ("state.age", "object"): "?",
        }

        for (col, dtype), default_value in required_cols.items():
            if col not in transitions:
                transitions[col] = default_value
            else:
                transitions = transitions.astype({col: dtype})
                transitions.loc[transitions[col].isna(), col] = default_value

        for classifier in (
            c for c in transitions.columns if c.startswith("classifiers.")
        ):
            transitions.loc[transitions[classifier].isna(), classifier] = "?"

        events = events.drop(columns="transition").astype(
            {"disturbed_transition_id": "int"}
        )
        
        self._next_transition_id = int(transitions["id"].max() + 1)

        return events, transitions
