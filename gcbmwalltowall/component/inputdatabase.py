import json
import pandas as pd
from os.path import relpath
from urllib.parse import quote_plus
from contextlib import contextmanager
from numbers import Number
from subprocess import run
from sqlalchemy import create_engine
from sqlalchemy import table
from sqlalchemy import column
from pathlib import Path

class InputDatabase:

    def __init__(self, aidb_path, yield_path, yield_interval):
        self.aidb_path = Path(aidb_path).absolute()
        self.yield_path = Path(yield_path).absolute()
        self.yield_interval = yield_interval

    def create(self, recliner2gcbm_exe, classifiers, output_path, transition_rules_path=None):
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

        recliner_config = {
            "Project": {
                "Mode": 0,
                "Configuration": 0
            },
            "OutputConfiguration": {
                "Name": "SQLite",
                "Parameters": {
                    "path": output_path.name
                }
            },
            "AIDBPath": relpath(self.aidb_path, output_dir),
            "ClassifierSet": [
                classifier.to_recliner(output_dir) for classifier in classifiers
            ],
            "GrowthCurves": {
                "Path": relpath(self.yield_path, output_dir),
                "Page": 0,
                "Header": True,
                "SpeciesCol": self._find_species_col(),
                "IncrementStartCol": increment_start_col,
                "IncrementEndCol": increment_end_col,
                "Interval": self.yield_interval,
                "Classifiers": [{
                    "Name": classifier.name,
                    "Column": self._find_classifier_col(classifier)
                } for classifier in classifiers]
            },
            "TransitionRules": {
                "Path": relpath(transition_rules_path, output_dir) if transition_rules_path else "",
                "Page": 0,
                "Header": True,
                "NameCol": 0,
                "AgeCol": 2,
                "DelayCol": 1,
                "TypeCol": None,
                "RuleDisturbanceTypeCol": None,
                "Classifiers": [{
                    "Name": classifier.name,
                    "Column": self._find_transition_col(transition_rules_path, classifier)
                } for classifier in classifiers],
                "RuleClassifiers": [{
                    "Name": classifier.name,
                    "Column": None
                } for classifier in classifiers]
            }
        }

        recliner2gcbm_config_path = output_path.with_suffix(".json")

        json.dump(
            recliner_config,
            open(recliner2gcbm_config_path, "w", newline="", encoding="utf-8"),
            ensure_ascii=False, indent=4)

        run([str(recliner2gcbm_exe), "-c", str(recliner2gcbm_config_path)])

    def get_disturbance_types(self):
        with self._connect() as conn:
            dist_type_table = table("tbldisturbancetypedefault", column("disttypename"))
            dist_types = {
                row[0] for row in conn.execute(
                    dist_type_table
                        .select(dist_type_table.c.disttypename)
                        .distinct()
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
                        last_total_increment = yield_table.iloc[:, increment_end_col].sum()
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
        with self._connect() as conn:
            species_type_table = table("tblspeciestypedefault", column("speciestypename"))
            species_types = {
                row[0] for row in conn.execute(
                    species_type_table
                        .select(species_type_table.c.speciestypename)
                        .distinct()
                )
            }

        yield_table = pd.read_csv(self.yield_path)
        for col in yield_table.columns:
            yield_col_values = set(yield_table[col].unique())
            if yield_col_values.issubset(species_types):
                return yield_table.columns.get_loc(col)

        raise RuntimeError(
            f"Unable to find species type column in {self.yield_path} "
            f"matching AIDB: {self.aidb_path}")

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

        raise RuntimeError(
            f"Unable to find matching column for classifier '{classifier.name}' "
            "in {self.yield_path}")

    def _find_transition_col(self, transition_rules_path, classifier):
        if not transition_rules_path:
            return -1

        transition_rules = pd.read_csv(transition_rules_path)
        if classifier.name not in transition_rules:
            return -1

        return transition_rules.columns.get_loc(classifier.name)

    def _only_numeric(self, values):
        return all((isinstance(v, Number) for v in values))

    @contextmanager
    def _connect(self):
        connection_string = quote_plus(
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={self.aidb_path};"
            r"ExtendedAnsiSQL=1;"
        )

        engine = create_engine(f"access+pyodbc:///?odbc_connect={connection_string}")

        try:
            with engine.connect() as conn:
                yield conn
        finally:
            engine.dispose()
