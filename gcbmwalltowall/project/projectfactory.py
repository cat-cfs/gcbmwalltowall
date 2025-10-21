from gcbmwalltowall.component.boundingbox import BoundingBox
from gcbmwalltowall.component.classifier import Classifier, DefaultClassifier
from gcbmwalltowall.component.cohort import Cohort
from gcbmwalltowall.component.disturbance import Disturbance
from gcbmwalltowall.component.inputdatabase import InputDatabase
from gcbmwalltowall.component.layer import DefaultLayer, Layer
from gcbmwalltowall.component.project import Project
from gcbmwalltowall.component.rollback import Rollback
from gcbmwalltowall.component.transition import Transition
from gcbmwalltowall.validation.generic import require_instance_of
from gcbmwalltowall.validation.string import require_not_null


class ProjectFactory:

    _layer_reserved_keywords = {
        "layer",
        "lookup_table",
        "attribute",
        "values_path",
        "values_col",
        "yield_col",
    }

    _disturbance_reserved_keywords = {
        "year",
        "disturbance_type",
        "age_after",
        "regen_delay",
        "lookup_table",
        "pattern",
        "metadata_attributes",
        "transition_undisturbed",
        "proportion",
    }

    def create(self, config):
        project_name = require_not_null(config.get("project_name"))

        if not config.get("bounding_box") and not config.get("layers"):
            raise RuntimeError(
                "Project requires a bounding_box entry or at least one item in "
                "the layers section"
            )

        bounding_box = self._create_bounding_box(config)
        input_db = self._create_input_database(config)
        classifiers = self._create_classifiers(config)
        layers = self._create_general_layers(config)
        disturbances, rule_based_disturbances = self._create_disturbances(config, classifiers, input_db)
        rollback = self._create_rollback(config, layers)

        transition_rules_disturbed = config.get("transition_rules") or config.get("transition_rules_disturbed")
        if transition_rules_disturbed:
            transition_rules_disturbed = config.resolve(transition_rules_disturbed)

        transition_rules_undisturbed = config.get("transition_rules_undisturbed")
        if transition_rules_undisturbed:
            transition_rules_undisturbed = config.resolve(transition_rules_undisturbed)

        cohorts = self._create_cohorts(config)
        dist_rules_path = config.get("disturbance_rules")
        if dist_rules_path:
            dist_rules_path = config.resolve(dist_rules_path)

        return Project(
            project_name,
            bounding_box,
            classifiers,
            layers,
            input_db,
            str(config.working_path),
            disturbances,
            rollback,
            transition_rules_disturbed,
            transition_rules_undisturbed,
            cohorts,
            config.get("max_workers"),
            config.get("max_mem_gb"),
            rule_based_disturbances,
            dist_rules_path,
        )

    def _extract_attribute(self, config):
        attribute = config.get("attribute")
        if not attribute:
            return None, None

        attribute_filter = None
        if isinstance(attribute, dict):
            attribute, filter_value = next(iter(attribute.items()))
            attribute_filter = {attribute: filter_value}

        return attribute, attribute_filter

    def _create_bounding_box(self, config):
        bounding_box_config = (
            config.get("bounding_box")
            or config.get("layers", {}).get("initial_age")
            or next(iter(config.get("layers", {}).values()))
        )

        bounding_box_layer = self._create_layer(
            config, "bounding_box", bounding_box_config
        )
        resolution = config.get("resolution")
        epsg = config.get("epsg")
        bounding_box = BoundingBox(bounding_box_layer, epsg, resolution)

        return bounding_box

    def _create_input_database(self, config):
        input_db = InputDatabase(
            config.resolve(require_not_null(config.get("aidb"))),
            config.resolve(require_not_null(config.get("yield_table"))),
            require_instance_of(config.get("yield_interval"), int),
        )

        return input_db

    def _create_classifiers(self, config):
        classifier_config = require_instance_of(config.get("classifiers"), dict)
        classifiers = [
            self._create_classifier(config, classifier_name, classifier_details)
            for classifier_name, classifier_details in classifier_config.items()
        ]

        return classifiers

    def _create_classifier(self, config, classifier_name, classifier_details):
        if not isinstance(classifier_details, dict):
            return DefaultClassifier(classifier_name, classifier_details)

        layer_path = config.resolve(require_not_null(classifier_details.get("layer")))
        layer_lookup_table = classifier_details.get(
            "lookup_table"
        ) or config.find_lookup_table(layer_path)

        attribute, attribute_filter = self._extract_attribute(classifier_details)

        layer = Layer(
            classifier_name,
            layer_path,
            attribute,
            config.resolve(layer_lookup_table) if layer_lookup_table else None,
            attribute_filter,
            **{
                k: v
                for k, v in classifier_details.items()
                if k not in self._layer_reserved_keywords
            },
        )

        return Classifier(
            layer,
            config.resolve(
                classifier_details.get("values_path", config["yield_table"])
            ),
            classifier_details.get("values_col"),
            classifier_details.get("yield_col"),
        )

    def _create_general_layers(self, config):
        layers = [
            self._create_layer(config, layer_name, layer_details)
            for layer_name, layer_details in config.get("layers", {}).items()
        ]

        return layers

    def _create_layer(self, config, layer_name, layer_details):
        if isinstance(layer_details, dict):
            layer_path = config.resolve(require_not_null(layer_details.get("layer")))
            layer_lookup_table = layer_details.get(
                "lookup_table"
            ) or config.find_lookup_table(layer_path)

            attribute, attribute_filter = self._extract_attribute(layer_details)

            return Layer(
                layer_name,
                layer_path,
                attribute,
                config.resolve(layer_lookup_table) if layer_lookup_table else None,
                attribute_filter,
                **{
                    k: v
                    for k, v in layer_details.items()
                    if k not in self._layer_reserved_keywords
                },
            )

        layer_path = None
        try:
            layer_path = config.resolve(layer_details)
        except:
            pass

        if layer_path and layer_path.exists():
            return Layer(
                layer_name,
                layer_path,
                lookup_table=config.find_lookup_table(layer_path),
            )
        else:
            # Maybe this is a fixed value, in which case we need a dummy layer.
            return DefaultLayer(layer_name, layer_details)

    def _create_disturbances(self, config, classifiers, input_db):
        disturbances = []
        rule_based_disturbances = []
        for pattern_or_name, dist_config in config.get("disturbances", {}).items():
            if isinstance(dist_config, str):
                if dist_config.endswith(".csv"):
                    rule_based_disturbances.append(config.resolve(dist_config))
                else:
                    disturbance_pattern = dist_config
                    disturbances.append(
                        Disturbance(
                            config.resolve(disturbance_pattern),
                            input_db,
                            name=pattern_or_name,
                        )
                    )
            else:
                transition_disturbed = None
                if dist_config.get("age_after") is not None:
                    transition_disturbed = Transition(
                        dist_config["age_after"],
                        dist_config.get("regen_delay", 0),
                        {
                            c.name: dist_config[c.name]
                            for c in classifiers
                            if c.name in dist_config
                        },
                    )

                transition_undisturbed = None
                transition_undisturbed_config = dist_config.get("transition_undisturbed")
                if transition_undisturbed_config:
                    transition_undisturbed = Transition(
                        transition_undisturbed_config.get("age_after"),
                        transition_undisturbed_config.get("regen_delay", 0),
                        {
                            c.name: transition_undisturbed_config[c.name]
                            for c in classifiers
                            if c.name in transition_undisturbed_config
                        },
                    )

                disturbances.append(
                    Disturbance(
                        config.resolve(dist_config.get("pattern", pattern_or_name)),
                        input_db,
                        dist_config.get("year"),
                        dist_config.get("disturbance_type"),
                        transition_disturbed,
                        transition_undisturbed,
                        config.resolve(
                            dist_config.get("lookup_table", config.config_path)
                        ),
                        name=pattern_or_name if "pattern" in dist_config else None,
                        metadata_attributes=dist_config.get("metadata_attributes"),
                        proportion=dist_config.get("proportion"),
                        **{
                            k: v
                            for k, v in dist_config.items()
                            if k not in self._disturbance_reserved_keywords
                            and k
                            not in {
                                c.name for c in classifiers if c.name in dist_config
                            }
                        },
                    )
                )

        return disturbances, rule_based_disturbances

    def _create_rollback(self, config, project_layers):
        rollback_config = config.get("rollback")
        if not rollback_config:
            return None

        age_distribution = config.resolve(
            require_not_null(rollback_config.get("age_distribution"))
        )
        rollback_year = rollback_config.get("rollback_year", 1990)

        inventory_year = rollback_config.get("inventory_year")
        inventory_year_layer = None
        if isinstance(inventory_year, str):
            layer_path = config.resolve(inventory_year)
            inventory_year_layer = Layer(
                "inventory_year",
                layer_path,
                lookup_table=config.find_lookup_table(layer_path),
            )
        elif isinstance(inventory_year, dict):
            layer_path = config.resolve(require_not_null(inventory_year.get("layer")))
            layer_lookup_table = inventory_year.get(
                "lookup_table"
            ) or config.find_lookup_table(layer_path)

            inventory_year_layer = Layer(
                "inventory_year",
                layer_path,
                inventory_year.get("attribute"),
                config.resolve(layer_lookup_table) if layer_lookup_table else None,
            )

        if inventory_year_layer:
            project_layers.append(inventory_year_layer)

        establishment_disturbance_type = rollback_config.get(
            "establishment_disturbance_type", "Wildfire"
        )

        if config.resolve(establishment_disturbance_type).exists():
            establishment_disturbance_type = config.resolve(
                establishment_disturbance_type
            )

        rollback = Rollback(
            age_distribution,
            inventory_year_layer.name if inventory_year_layer else inventory_year,
            rollback_year,
            rollback_config.get("prioritize_disturbances", False),
            rollback_config.get("single_draw", False),
            establishment_disturbance_type,
            config.gcbm_disturbance_order_path,
        )

        return rollback

    def _create_cohorts(self, config):
        all_cohort_config = config.get("cohorts")
        if not all_cohort_config:
            return None

        proportion_layer_name = "cohort_proportion"
        if proportion_layer_name not in config["layers"]:
            raise RuntimeError("Base cohort requires a cohort_proportion layer")

        cohorts = []
        for cohort_config in all_cohort_config:
            cohort_layers = []
            cohort_classifiers = []
            for layer_name, layer_config in cohort_config.items():
                if layer_name in config["layers"]:
                    cohort_layers.append(
                        self._create_layer(config, layer_name, layer_config)
                    )
                elif layer_name in config["classifiers"]:
                    cohort_classifiers.append(
                        self._create_classifier(config, layer_name, layer_config)
                    )
                else:
                    raise RuntimeError(
                        f"{layer_name} in cohort must override a base layer"
                    )

            cohorts.append(Cohort(cohort_layers, cohort_classifiers))

        return cohorts
