import pandas as pd
from tempfile import TemporaryDirectory
from pathlib import Path
from arrow_space.flattened_coordinate_dataset import FlattenedCoordinateDataset
from cbm4.app.spatial.gcbm_input.gcbm_disturbance_preprocessor import GCBMInputReader


class GCBMDisturbanceInputReader(GCBMInputReader):

    def __init__(
        self,
        gcbm_disturbance_dataset: FlattenedCoordinateDataset,
        cbm4_defaults_path: str,
        cbm4_defaults_locale: str,
    ):
        self._temp_dir = TemporaryDirectory()
        self._gcbm_disturbance_dataset = gcbm_disturbance_dataset
        self._cbm_defaults_path = cbm4_defaults_path
        self._cbm_defaults_locale = cbm4_defaults_locale
        self._input_datasets_by_cohort = {0: gcbm_disturbance_dataset}
        self._disturbance_type_map = self._get_disturbance_map()
        self._disturbance_layer_names = self._get_disturbance_layer_names()

    @property
    def transition_rules_disturbed(self) -> pd.DataFrame | None:
        return self._read_table("transition_rules_disturbed")

    @property
    def transition_rules_undisturbed(self) -> pd.DataFrame | None:
        return self._read_table("transition_rules_undisturbed")

    @property
    def transitions_disturbed(self) -> pd.DataFrame | None:
        return self._read_table("transitions_disturbed")

    @property
    def transitions_undisturbed(self) -> pd.DataFrame | None:
        return self._read_table("transitions_undisturbed")

    @property
    def cohort_filter(self) -> pd.DataFrame | None:
        return self._read_table("cohort_filter")

    @property
    def cohort_sort(self) -> pd.DataFrame | None:
        return self._read_table("cohort_sort")

    @property
    def rule_based_disturbances(self) -> pd.DataFrame | None:
        return self._read_table("events")

    @property
    def disturbance_rules_path(self) -> str:
        disturbance_rules_path = "null"
        if self._gcbm_disturbance_dataset.file_or_dir_exists("disturbance_rules"):
            disturbance_rules_path = str(
                Path(self._temp_dir.name).joinpath("disturbance_rules.json")
            )

            self._gcbm_disturbance_dataset.extract_file_or_dir(
                "disturbance_rules", disturbance_rules_path
            )

        return disturbance_rules_path

    def _read_table(self, table_name: str) -> pd.DataFrame | None:
        if not self._gcbm_disturbance_dataset.table_exists(table_name):
            return None

        return self._gcbm_disturbance_dataset.read_table_pandas(table_name)