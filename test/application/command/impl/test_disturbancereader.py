import gcbmwalltowall
import pandas as pd
from pathlib import Path
from pytest import fixture
from unittest.mock import create_autospec
from gcbmwalltowall.application.command.impl.disturbancereader import DisturbanceReader
from gcbmwalltowall.application.command.impl.disturbanceformatter import DisturbanceFormatter
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset


@fixture
def extra_disturbances_dataset() -> FlattenedCoordinateDataset:
    ds_path = str(
        Path(gcbmwalltowall.__file__).parent.parent.joinpath(
            "test", "resources", "extra_disturbances_dataset"
        )
    )

    return FlattenedCoordinateDataset("disturbance", "local_storage", ds_path)


@fixture
def disturbance_formatter() -> DisturbanceFormatter:
    def format_fn(df: pd.DataFrame):
        df.rename(
            columns={"transition": "disturbed_transition_id"},
            inplace=True
        )

    formatter = create_autospec(DisturbanceFormatter)
    formatter.format.side_effect = format_fn

    return formatter
    

def test_read_disturbances(extra_disturbances_dataset, disturbance_formatter):
    reader = DisturbanceReader(extra_disturbances_dataset, disturbance_formatter)
    disturbance_data = reader.read_disturbances()
    assert disturbance_data is not None
    assert not disturbance_data.empty
    assert {"year", "disturbance_type"}.issubset(set(disturbance_data.columns.tolist()))


def test_read_transitions(extra_disturbances_dataset, disturbance_formatter):
    reader = DisturbanceReader(extra_disturbances_dataset, disturbance_formatter)
    transition_data = reader.read_transitions()
    assert transition_data is not None
    assert len(transition_data) > 0


def test_starting_from(cbm4_project, extra_disturbances_dataset):
    reader = DisturbanceReader.starting_from(cbm4_project, extra_disturbances_dataset)
    disturbance_data = reader.read_disturbances()
    assert disturbance_data is not None
    original_max_transition_id = cbm4_project.get_max_transition_id()
    extra_disturbances_min_transition_id = disturbance_data["disturbed_transition_id"].max()
    assert extra_disturbances_min_transition_id > original_max_transition_id


def test_transition_offset(extra_disturbances_dataset, disturbance_formatter):
    transition_offset = 100
    reader = DisturbanceReader(
        extra_disturbances_dataset, disturbance_formatter, transition_offset
    )

    disturbance_data = reader.read_disturbances()
    assert disturbance_data is not None
    min_addon_transition_id = disturbance_data["disturbed_transition_id"].min()
    assert min_addon_transition_id > transition_offset
