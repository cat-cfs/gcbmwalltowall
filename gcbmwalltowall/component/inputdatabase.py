from numbers import Number

import pandas as pd
from gcbminputloader.project.project import ProjectType
from gcbminputloader.project.projectfactory import ProjectFactory
from gcbminputloader.util.configuration import Configuration
from gcbminputloader.util.db import get_connection
from sqlalchemy import text

from gcbmwalltowall.util.path import Path


class InputDatabase:

    def __init__(self, aidb_path, yield_path, yield_interval):
        self.aidb_path = Path(aidb_path).absolute()
        self.yield_path = Path(yield_path).absolute()
        self.yield_interval = yield_interval

    def create(self, classifiers, output_path, transition_rules_path=None):
        output_path = Path(output_path).absolute()
        input_db_config_path = output_path.with_suffix(".json")
        output_dir = Path(output_path).absolute().parent

        # Add any missing classifier columns to the transition rules.
        if transition_rules_path and Path(transition_rules_path).exists():
            transitions = pd.read_csv(transition_rules_path)
            changed = False
            for classifier in classifiers:
                if classifier.name not in transitions:
                    transitions[classifier.name] = "?"
                    changed = True

            if changed:
                transitions.to_csv(transition_rules_path, index=False)
        else:
            transition_rules_path = None

        increment_start_col, increment_end_col = self._find_increment_cols()

        input_db_config = Configuration(
            {
                "aidb": self.aidb_path,
                "classifiers": [c.name for c in classifiers],
                "features": {
                    "growth_curves": {
                        "path": self.yield_path,
                        "interval": self.yield_interval,
                        "aidb_species_col": self._find_species_col(),
                        "increment_start_col": increment_start_col,
                        "increment_end_col": increment_end_col,
                        "classifier_cols": {
                            c.name: self._find_classifier_col(c) for c in classifiers
                        },
                    }
                },
            },
            output_dir,
        )

        if transition_rules_path and Path(transition_rules_path).exists():
            # gcbmwalltowall expects a specific naming convention for transition rules:
            #   - id, regen_delay, age_after, age_reset_type, disturbance_type
            #   - exact classifier name: the classifier values to transition to
            #   - exact classifier name with "_match" suffix: the classifier values to match
            #       if supplying rule-based transitions
            input_db_config["features"]["transition_rules"] = {
                "path": transition_rules_path,
                "id_col": self._find_col_index(transition_rules_path, "id"),
                "reset_age_col": self._find_col_index(
                    transition_rules_path, "age_after"
                ),
                "reset_age_type_col": self._find_col_index(
                    transition_rules_path, "age_reset_type"
                ),
                "regen_delay_col": self._find_col_index(
                    transition_rules_path, "regen_delay"
                ),
                "classifier_transition_cols": {
                    c.name: self._find_transition_col(transition_rules_path, c)
                    for c in classifiers
                },
                "disturbance_type_col": self._find_col_index(
                    transition_rules_path, "disturbance_type"
                ),
                "classifier_matching_cols": {
                    c.name: self._find_col_index(
                        transition_rules_path, f"{c.name}_match"
                    )
                    for c in classifiers
                },
            }

        input_db_type = (
            ProjectType.LegacyGcbmClassicSpatial
            if self.aidb_path.suffix == ".mdb"
            else ProjectType.GcbmClassicSpatial
        )

        input_db = ProjectFactory().from_config(input_db_type, input_db_config)
        input_db.save(input_db_config_path)
        input_db.create(str(output_path))

    def get_disturbance_types(self):
        with get_connection(self.aidb_path) as conn:
            if self.aidb_path.suffix == ".mdb":
                dist_types = {
                    row[0]
                    for row in conn.execute(
                        text(
                            f"SELECT DISTINCT disttypename FROM tbldisturbancetypedefault"
                        )
                    )
                }
            else:
                dist_types = {
                    row[0]
                    for row in conn.execute(
                        text(
                            """
                        SELECT DISTINCT name
                        FROM disturbance_type dt
                        INNER JOIN disturbance_type_tr d_tr
                            ON dt.id = d_tr.disturbance_type_id
                        WHERE locale_id = 1
                        """
                        )
                    )
                }

            return dist_types

    def _find_increment_cols(self):
        # Look for a run of at least 5 columns where the values are all numeric,
        # the first column's values are all zero, and the values in the final
        # column decline by no more than 50%.
        yield_table = pd.read_csv(self.yield_path)
        yield_columns = yield_table.columns
        numeric_col_run = 0
        increment_start_col = -1
        increment_end_col = -1
        for col in yield_columns:
            is_numeric = self._only_numeric(yield_table[col].unique())
            if is_numeric:
                if numeric_col_run == 0:
                    if yield_table[col].sum() == 0:
                        increment_start_col = yield_columns.get_loc(col)
                        numeric_col_run += 1
                else:
                    if numeric_col_run >= 5:
                        last_total_increment = yield_table.iloc[
                            :, increment_end_col
                        ].sum()
                        this_total_increment = yield_table[col].sum()
                        if this_total_increment < last_total_increment * 0.5:
                            break

                    increment_end_col = yield_columns.get_loc(col)
                    numeric_col_run += 1
            else:
                if numeric_col_run >= 5:
                    return (increment_start_col, increment_end_col)

                numeric_col_run = 0

        if numeric_col_run >= 5:
            return (increment_start_col, increment_end_col)

        raise RuntimeError(f"Unable to find increment columns in {self.yield_path}")

    def _find_species_col(self):
        with get_connection(self.aidb_path) as conn:
            if self.aidb_path.suffix == ".mdb":
                species_types = {
                    row[0].lower()
                    for row in conn.execute(
                        text(
                            "SELECT DISTINCT speciestypename FROM tblspeciestypedefault"
                        )
                    )
                }
            else:
                species_types = {
                    row[0].lower()
                    for row in conn.execute(
                        text(
                            """
                        SELECT DISTINCT name
                        FROM species s
                        INNER JOIN species_tr s_tr
                            ON s.id = s_tr.species_id
                        WHERE locale_id = 1
                        """
                        )
                    )
                }

        yield_table = pd.read_csv(self.yield_path)
        for col in yield_table.columns:
            yield_col_values = {str(v).lower() for v in yield_table[col].unique()}
            if yield_col_values.issubset(species_types):
                return yield_table.columns.get_loc(col)

        raise RuntimeError(
            f"Unable to find species type column in {self.yield_path} "
            f"matching AIDB: {self.aidb_path}"
        )

    def _find_classifier_col(self, classifier):
        # Configured yield column number.
        if isinstance(classifier.yield_col, Number):
            return classifier.yield_col

        # Configured yield column name.
        yield_table = pd.read_csv(self.yield_path)
        if classifier.yield_col:
            return yield_table.columns.get_loc(classifier.yield_col)

        # Classifier values come from yield table, classifier values column configured.
        if classifier.values_path == self.yield_path:
            if isinstance(classifier.values_col, Number):
                return classifier.values_col
            elif classifier.values_col:
                return yield_table.columns.get_loc(classifier.values_col)

        # Search for a column name matching the classifier name.
        if classifier.name in yield_table.columns:
            return yield_table.columns.get_loc(classifier.name)

        # Finally, see if there's a column in the yield table which is a subset
        # of all possible values for the classifier, excluding wildcards.
        classifier_values = {str(v) for v in classifier.values} - {"?"}
        for col in yield_table.columns:
            yield_column_values = {str(v) for v in yield_table[col].unique()} - {"?"}
            if yield_column_values.issubset(classifier_values):
                return yield_table.columns.get_loc(col)

        # Finally, if this is a default (non-spatial/dummy) classifier, allow it to
        # have no column mapping.
        if classifier.is_default:
            return None

        raise RuntimeError(
            f"Unable to find matching column for classifier '{classifier.name}' "
            f"in {self.yield_path}"
        )

    def _find_transition_col(self, transition_rules_path, classifier):
        return self._find_col_index(transition_rules_path, classifier.name)

    def _find_col_index(self, csv_path, col_name, default=None):
        if not (csv_path and csv_path.exists()):
            return default

        header = pd.read_csv(csv_path, nrows=1)
        if col_name not in header:
            return default

        return header.columns.get_loc(col_name)

    def _only_numeric(self, values):
        return all((isinstance(v, Number) for v in values))
