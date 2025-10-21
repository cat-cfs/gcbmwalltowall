import csv
import logging
import shutil
import pandas as pd
from datetime import date
from itertools import chain
from multiprocessing import cpu_count
from tempfile import TemporaryDirectory
from uuid import uuid4

from mojadata.cleanup import cleanup
from mojadata.gdaltiler2d import GdalTiler2D
from mojadata.layer.gcbm.transitionrulemanager import \
    SharedTransitionRuleManager
from mojadata.util import gdal

from gcbmwalltowall.component.boundingbox import BoundingBox
from gcbmwalltowall.component.inputdatabase import InputDatabase
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.validation.generic import require_instance_of
from gcbmwalltowall.validation.string import require_not_null


class Project:

    def __init__(
        self,
        name,
        bounding_box,
        classifiers,
        layers,
        input_db,
        output_path,
        disturbances=None,
        rollback=None,
        transition_rules_disturbed_path=None,
        transition_rules_undisturbed_path=None,
        cohorts=None,
        max_workers=None,
        max_mem_gb=None,
        rule_based_disturbances=None,
        disturbance_rules=None,
    ):
        self.name = require_not_null(name)
        self.bounding_box = require_instance_of(bounding_box, BoundingBox)
        self.classifiers = require_instance_of(classifiers, list)
        self.layers = require_instance_of(layers, list)
        self.input_db = require_instance_of(input_db, InputDatabase)
        self.output_path = Path(require_not_null(output_path)).absolute()
        self.disturbances = disturbances
        self.rollback = rollback
        self.transition_rules_disturbed_path = (
            Path(transition_rules_disturbed_path).absolute()
            if transition_rules_disturbed_path
            else None
        )

        self.transition_rules_undisturbed_path = (
            Path(transition_rules_undisturbed_path).absolute()
            if transition_rules_undisturbed_path
            else None
        )

        self.cohorts = cohorts
        self.max_workers = max_workers
        self.max_mem_gb = max_mem_gb
        self.rule_based_disturbances = rule_based_disturbances
        self.disturbance_rules = disturbance_rules

    @property
    def tiler_output_path(self):
        return self.output_path.joinpath("layers", "tiled")

    @property
    def rollback_output_path(self):
        return self.output_path.joinpath("layers", "rollback")

    @property
    def input_db_path(self):
        return self.output_path.joinpath("input_database", "gcbm_input.db")

    @property
    def rollback_input_db_path(self):
        return self.output_path.joinpath("input_database", "rollback_gcbm_input.db")

    def tile(self):
        shutil.rmtree(str(self.tiler_output_path), ignore_errors=True)
        shutil.rmtree(str(self.rollback_output_path), ignore_errors=True)
        self.tiler_output_path.mkdir(parents=True, exist_ok=True)

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()
        with cleanup():
            logging.info(f"Preparing non-disturbance layers")
            tiler_bbox = self.bounding_box.to_tiler_layer(rule_manager)
            tiler_layers = [
                self._make_tiler_layer(rule_manager, layer)
                for layer in chain(self.layers, self.classifiers)
            ]

            logging.info(f"Finished preparing non-disturbance layers")
            if self.disturbances:
                for disturbance in self.disturbances:
                    logging.info(f"Preparing {disturbance.name or disturbance.pattern}")
                    layer = disturbance.to_tiler_layer(rule_manager)
                    if isinstance(layer, list):
                        tiler_layers.extend(layer)
                    else:
                        tiler_layers.append(layer)

                    logging.info(
                        f"Finished preparing {disturbance.name or disturbance.pattern}"
                    )

            logging.info("Starting up tiler...")
            tiler_mem = (self.max_mem_gb * 1024**3) if self.max_mem_gb else None
            tiler = GdalTiler2D(
                tiler_bbox,
                use_bounding_box_resolution=True,
                workers=min((self.max_workers or cpu_count()), len(tiler_layers)),
                total_mem_bytes=tiler_mem,
            )

            tiler.tile(tiler_layers, str(self.tiler_output_path))
            if self.cohorts:
                for i, cohort in enumerate(self.cohorts, 1):
                    cohort_output_path = self.tiler_output_path.joinpath(
                        rf"cohorts\{i}"
                    )
                    cohort_layers = [
                        self._make_tiler_layer(rule_manager, layer)
                        for layer in chain(cohort.layers, cohort.classifiers)
                    ]

                    tiler.tile(cohort_layers, str(cohort_output_path))

            rule_manager.write_rules(
                str(self.tiler_output_path.joinpath("transition_rules.csv"))
            )

    def create_input_database(self):
        output_path = self.input_db_path.parent
        output_path.mkdir(parents=True, exist_ok=True)
        prepared_transition_rules_path = output_path.joinpath(
            "gcbmwalltowall_transitions.csv"
        )
        tiler_transition_rules_path = self.tiler_output_path.absolute()
        self._prepare_transition_rules(
            tiler_transition_rules_path, prepared_transition_rules_path
        )
        self.input_db.create(
            self.classifiers, self.input_db_path, prepared_transition_rules_path
        )
        self._prepare_extra_data(output_path)

    def run_rollback(self):
        if not self.rollback:
            return

        mgr = SharedTransitionRuleManager()
        mgr.start()
        rule_manager = mgr.TransitionRuleManager()

        output_path = self.input_db_path.parent
        rollback_transition_rules_path = self.rollback_output_path.absolute()
        rollback_mem = (self.max_mem_gb // 8) if self.max_mem_gb else None
        self.rollback.run(
            self.classifiers,
            self.tiler_output_path,
            self.input_db_path,
            rule_manager,
            rollback_mem,
        )

        if self.cohorts:
            for i, _ in enumerate(self.cohorts, 1):
                with TemporaryDirectory() as tmp:
                    cohort_staging_path = Path(tmp)
                    staging_layers_path = cohort_staging_path.joinpath(r"layers\tiled")
                    staging_layers_path.mkdir(parents=True)
                    cohort_layers = list(
                        (
                            fn
                            for fn in self.tiler_output_path.joinpath(
                                rf"cohorts\{i}"
                            ).glob("*.*")
                            if fn.name != "study_area.json"
                        )
                    )

                    cohort_layer_names = [fn.name for fn in cohort_layers]
                    for fn in self.tiler_output_path.glob("*.*"):
                        if fn.name not in cohort_layer_names:
                            shutil.copyfile(fn, staging_layers_path.joinpath(fn.name))

                    for fn in cohort_layers:
                        shutil.copyfile(fn, staging_layers_path.joinpath(fn.name))

                    shutil.copyfile(
                        self.input_db_path,
                        cohort_staging_path.joinpath("input_database"),
                    )
                    self.rollback.run(
                        self.classifiers,
                        staging_layers_path,
                        self.input_db_path,
                        rule_manager,
                        rollback_mem,
                    )

                    cohort_rollback_path = self.rollback_output_path.joinpath(
                        rf"cohorts\{i}"
                    )
                    cohort_rollback_path.mkdir(parents=True)
                    for fn in cohort_staging_path.joinpath(r"layers\rollback").glob(
                        "*.*"
                    ):
                        if "contemporary" not in str(fn):
                            shutil.copyfile(fn, cohort_rollback_path.joinpath(fn.name))

        final_transition_rules_path = output_path.joinpath(
            "gcbmwalltowall_rollback_transitions.csv"
        )
        self._prepare_transition_rules(
            rollback_transition_rules_path, final_transition_rules_path
        )
        self.input_db.create(
            self.classifiers, self.rollback_input_db_path, final_transition_rules_path
        )

    def configure_gcbm(
        self,
        template_path,
        disturbance_order=None,
        start_year=1990,
        end_year=date.today().year,
    ):
        exclusions_file = next(self.rollback_output_path.glob("exclusions.txt"), None)
        excluded_layers = (
            [line[0] for line in csv.reader(open(exclusions_file))]
            if exclusions_file
            else None
        )

        input_db_path = (
            self.rollback_input_db_path if exclusions_file else self.input_db_path
        )

        layer_paths = [str(self.tiler_output_path)]
        if exclusions_file:
            layer_paths.append(str(self.rollback_output_path))

        configurer = GCBMConfigurer(
            layer_paths,
            template_path,
            input_db_path,
            self.output_path.joinpath("gcbm_project"),
            start_year,
            end_year,
            disturbance_order,
            excluded_layers,
        )

        configurer.configure()

    def _make_tiler_layer(self, rule_manager, walltowall_layer):
        return walltowall_layer.to_tiler_layer(
            rule_manager,
            # For spatial rollback compatibility:
            data_type=(
                gdal.GDT_Int16
                if getattr(walltowall_layer, "name", "") == "initial_age"
                else None
            ),
        )

    def _prepare_transition_rules(self, tiler_output_path, output_path):
        output_fn_parts = output_path.name.split("_", 1)
        transition_undisturbed_path = output_path.with_name(
            "_".join((output_fn_parts[0], "undisturbed", output_fn_parts[1]))
        )

        transition_rules_undisturbed_path = output_path.parent.joinpath(
            "gcbmwalltowall_transition_rules_undisturbed.csv"
        )

        for p in (output_path, transition_undisturbed_path, transition_rules_undisturbed_path):
            p.unlink(True)

        transition_disturbed_path = tiler_output_path.joinpath("transition_rules.csv")
        if not (
            transition_disturbed_path.exists() or self.transition_rules_disturbed_path
        ):
            return None

        all_transition_rules = []
        for transition_path in (
            transition_disturbed_path,
            self.transition_rules_disturbed_path,
        ):
            if transition_path and transition_path.exists():
                all_transition_rules.extend(
                    (row for row in csv.DictReader(open(transition_path, newline="")))
                )

        non_classifier_cols = {
            "id",
            "regen_delay",
            "age_after",
            "disturbance_type",
            "age_reset_type",
        }
        all_transition_classifiers = set()
        for transition in all_transition_rules:
            all_transition_classifiers.update(
                set(transition.keys()) - non_classifier_cols
            )

        for transition in all_transition_rules:
            transition["id"] = transition.get("id", str(uuid4()))
            transition["disturbance_type"] = transition.get("disturbance_type", "")
            transition["age_reset_type"] = transition.get("age_reset_type", "absolute")
            transition["regen_delay"] = transition.get("regen_delay", 0)

            for classifier in all_transition_classifiers:
                transition[classifier] = transition.get(classifier, "?")

            for classifier in self.classifiers:
                transition[classifier.name] = transition.get(classifier.name, "?")
                transition[f"{classifier.name}_match"] = transition.get(
                    f"{classifier.name}_match", ""
                )

        tiler_transition_undisturbed_path = tiler_output_path.joinpath(
            "undisturbed_transition_rules.csv"
        )
        if tiler_transition_undisturbed_path.exists():
            shutil.copyfile(str(tiler_transition_undisturbed_path), str(transition_undisturbed_path))

        if self.transition_rules_undisturbed_path:
            shutil.copyfile(
                str(self.transition_rules_undisturbed_path),
                str(transition_rules_undisturbed_path),
            )

        with open(output_path, "w", newline="") as merged_transition_rules:
            header = all_transition_rules[0].keys()
            writer = csv.DictWriter(merged_transition_rules, fieldnames=header)
            writer.writeheader()
            writer.writerows(all_transition_rules)

    def _prepare_extra_data(self, output_path):
        if self.disturbance_rules:
            shutil.copyfile(
                self.disturbance_rules,
                output_path.joinpath("disturbance_rules.json")
            )
        
        if self.rule_based_disturbances:
            rule_based_disturbances = pd.concat([
                pd.read_csv(rule_based_dist_path)
                for rule_based_dist_path in self.rule_based_disturbances
            ])

            rule_based_disturbances.to_csv(
                output_path.joinpath("rule_based_disturbances.csv"),
                index=False
            )
