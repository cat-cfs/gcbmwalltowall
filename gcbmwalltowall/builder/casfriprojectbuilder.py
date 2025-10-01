from __future__ import annotations

import csv
import logging
import string
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd

from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.util.path import Path, relpath


class CasfriProjectBuilder(ProjectBuilder):

    casfri_builder_keys = {
        "type",
        "casfri_data",
        "other_data",
        "dm_xls",
        "inventory_year",
        "age_distribution",
    }

    @staticmethod
    def build(config: Configuration) -> Configuration:
        builder_config = config["builder"]
        casfri_data = config.resolve(Path(builder_config["casfri_data"]))
        other_data = config.resolve(Path(builder_config["other_data"]))

        config["resolution"] = 0.001
        config["yield_interval"] = 10

        config["classifiers"] = {
            "RU": {
                "layer": relpath(
                    next(other_data.rglob("pspus_2016.shp")), config.working_path
                ),
                "attribute": "Reconcilia",
            },
            "LeadingSpecies": {
                "layer": relpath(
                    next(casfri_data.rglob("leading_species.tiff")), config.working_path
                )
            },
        }

        config["layers"] = {
            "initial_age": relpath(
                next(casfri_data.rglob("age_2022.tiff")), config.working_path
            ),
            "mean_annual_temperature": relpath(
                next(other_data.rglob("NAmerica_MAT_1971_2000.tif")),
                config.working_path,
            ),
            "admin_boundary": {
                "layer": relpath(
                    next(other_data.rglob("pspus_2016.shp")), config.working_path
                ),
                "attribute": "ProvinceNa",
            },
            "eco_boundary": {
                "layer": relpath(
                    next(other_data.rglob("pspus_2016.shp")), config.working_path
                ),
                "attribute": "EcoBound_1",
            },
        }

        dist_type_substitutions = CasfriProjectBuilder._read_casfri_subsitutions(
            config.resolve(builder_config["dm_xls"])
        )

        # Use only the first set of disturbance layers found.
        disturbance_data = next(casfri_data.rglob("disturbances_*.tiff"), None)
        if disturbance_data:
            if "disturbances" not in config:
                config["disturbances"] = {}

            disturbance_dir = disturbance_data.parent
            for disturbance_layer in disturbance_dir.glob("disturbances_*.tiff"):
                has_valid_disturbances = (
                    CasfriProjectBuilder._write_disturbance_attribute_table(
                        config,
                        disturbance_layer.with_suffix(".csv"),
                        dist_type_substitutions,
                    )
                )

                if has_valid_disturbances:
                    config["disturbances"][
                        relpath(disturbance_layer, config.working_path)
                    ] = {}

        age_distribution = config.resolve(
            builder_config.get("age_distribution", "age_distribution.json")
        )
        if age_distribution.exists():
            config["rollback"] = {
                "age_distribution": relpath(age_distribution, config.working_path),
                "inventory_year": builder_config.get("inventory_year", 2022),
                "prioritize_disturbances": True,
                "single_draw": True,
            }

        # Users can override or explicitly configure top-level items, or provide
        # extra values for items that are collections (i.e. layers, disturbances).
        for k, v in builder_config.items():
            if k in CasfriProjectBuilder.casfri_builder_keys:
                continue

            if isinstance(v, dict):
                if k in config:
                    config[k].update(v)
                else:
                    config[k] = v
            else:
                config[k] = v

        return config

    @staticmethod
    def _write_disturbance_attribute_table(
        config: Configuration,
        casfri_csv: Path,
        dist_type_substitutions: dict[str, list[Any]],
    ) -> bool:
        rows_written = 0
        with open(
            config.resolve_working(casfri_csv.name), "w", encoding="utf-8", newline=""
        ) as dist_layer_lookup:
            writer = csv.writer(dist_layer_lookup)
            writer.writerow(["px", "year", "disturbance_type", "reset_age"])
            for row in csv.DictReader(open(casfri_csv, "r")):
                gcbm_dist_types = dist_type_substitutions.get(row["dist_type"])
                if not gcbm_dist_types:
                    continue

                dist_year = int(row["dist_year"])
                if dist_year < 0:
                    continue

                if isinstance(gcbm_dist_types, list):
                    for (
                        mortality_upper_bound,
                        dist_type,
                        stand_replacing,
                    ) in gcbm_dist_types:
                        if int(row["dist_ext_upper"]) <= mortality_upper_bound:
                            gcbm_dist_type = dist_type
                            break
                else:
                    gcbm_dist_type, stand_replacing = gcbm_dist_types

                reset_age = 0 if stand_replacing else -1
                writer.writerow(
                    [int(row["raster_id"]), dist_year, gcbm_dist_type, reset_age]
                )
                rows_written += 1

        return rows_written > 0

    @staticmethod
    def _df_to_xls(row: int, col: int) -> str:
        return f"{string.ascii_uppercase[col]}{row + 1}"

    @staticmethod
    def _read_casfri_subsitutions(dm_xls: Path | str) -> dict[str, list[Any]]:
        dist_type_substitutions = defaultdict(list)

        sub_header = ["s", "s", "s"]
        sub_item = ["n", "s", "n"]

        sheets = pd.read_excel(dm_xls, header=None, sheet_name=None)
        for sheet, df in sheets.items():
            if sheet in ("README", "AIDB DMs"):
                continue

            casfri_dist_type = sheet

            # Find all the populated cells and whether they're a string or numeric type
            # to simplify detecting the disturbance type substitutions.
            search_mask = df.applymap(
                lambda val: (
                    (
                        "s"
                        if isinstance(val, str)
                        else (
                            "n"
                            if isinstance(val, float) or isinstance(val, int)
                            else None
                        )
                    )
                    if pd.notnull(val)
                    else None
                )
            )

            checked = pd.DataFrame(False, columns=df.columns, index=df.index)

            for r, row in search_mask.iterrows():
                for c, _ in enumerate(row):
                    # Skip previously checked cells.
                    if checked.loc[r, c]:
                        continue

                    checked.loc[r, c] = True

                    # Skip cells too close to the end of the sheet to form a matrix definition.
                    if c > len(row) - 3 or r == len(search_mask):
                        continue

                    """
                    The table of disturbance type substitutions from CASFRI to GCBM
                    is organized so that the tab name in Excel is the CASFRI disturbance
                    type, containing a table where the header is:
                    dist_ext_upper    Disturbance type    Stand replacing
                
                    Followed by one or more rows of:
                    <max dist_ext_upper value> <AIDB disturbance type> <Stand-replacing: TRUE/FALSE>
                    """
                    is_sub_table = np.all(
                        search_mask.loc[r:r, c : c + 2].values == sub_header
                    ) and np.all(
                        search_mask.loc[r + 1 : r + 1, c : c + 2].values == sub_item
                    )

                    if is_sub_table:
                        logging.info(
                            f"Disturbance type substitutions found in {sheet} at "
                            f"{CasfriProjectBuilder._df_to_xls(r, c)}"
                        )

                        checked.loc[r:r, c : c + 2] = True

                        for row_idx in range(r + 1, len(search_mask)):
                            if not np.all(
                                search_mask.loc[row_idx:row_idx, c : c + 2].values
                                == sub_item
                            ):
                                # The table is complete when the content pattern ends.
                                break

                            dist_type_substitutions[casfri_dist_type].append(
                                df.loc[row_idx:row_idx, c : c + 2].values.tolist()[0]
                            )

                            checked.loc[row_idx:row_idx, c : c + 2] = True

        return dist_type_substitutions
