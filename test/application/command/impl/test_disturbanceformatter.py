import pandas as pd
from unittest.mock import MagicMock
from unittest.mock import create_autospec
from random import randint
from gcbmwalltowall.application.command.impl.disturbanceformatter import DisturbanceFormatter
from cbm4.app.spatial.gcbm_input.timestep_interpreter import TimestepInterpreter


def test_format_defaults():
    cbm_defaults = MagicMock()
    cbm_defaults.disturbance_type_ref = [
        {
            "disturbance_type_name": "a",
            "disturbance_type_id": 10
        },
        {
            "disturbance_type_name": "b",
            "disturbance_type_id": 20
        },
    ]

    timestep_interpretation = create_autospec(TimestepInterpreter)
    timestep_interpretation.get_timestep.return_value = 999

    disturbance_events = pd.DataFrame({
        "year": [1999, 2000],
        "disturbance_type": ["a", "b"],
    })

    original_columns = set(disturbance_events.columns.to_list())

    formatter = DisturbanceFormatter(cbm_defaults, timestep_interpretation)
    formatter.format(disturbance_events)

    assert set(disturbance_events.columns.to_list()) == original_columns.union(
        {
            "disturbance_id",
            "disturbance_order",
            "proportion",
            "enable_merge",
            "sort_id",
            "filter_id",
            "undisturbed_transition_id",
            "disturbed_transition_id",
            "timestep",
            "default_disturbance_type_id",
        }
    )


def test_format_preserves_existing_attrs():
    cbm_defaults = MagicMock()
    cbm_defaults.disturbance_type_ref = [
        {
            "disturbance_type_name": "a",
            "disturbance_type_id": 10
        },
        {
            "disturbance_type_name": "b",
            "disturbance_type_id": 20
        },
    ]

    timestep_interpretation = create_autospec(TimestepInterpreter)
    timestep_interpretation.get_timestep.return_value = 999

    disturbance_events = pd.DataFrame({
        "year": [1999, 2000],
        "disturbance_type": ["a", "b"],
        "disturbance_id": [randint(0, 100), randint(0, 100)],
        "disturbance_order": [randint(0, 100), randint(0, 100)],
        "proportion": [1 / randint(1, 10), 1 / randint(1, 10)],
        "enable_merge": [True, False],
        "sort_id": [randint(0, 100), randint(0, 100)],
        "filter_id": [randint(0, 100), randint(0, 100)],
        "undisturbed_transition_id": [randint(0, 100), randint(0, 100)],
        "disturbed_transition_id": [randint(0, 100), randint(0, 100)],
    })

    original_columns = disturbance_events.columns.to_list()

    formatter = DisturbanceFormatter(cbm_defaults, timestep_interpretation)
    formatted_disturbance_events = disturbance_events.copy()
    formatter.format(formatted_disturbance_events)

    for col in original_columns:
        pd.testing.assert_series_equal(
            formatted_disturbance_events[col],  disturbance_events[col]
        )
