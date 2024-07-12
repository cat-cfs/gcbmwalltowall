from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from multiprocessing import Manager
from pathlib import Path
from sqlalchemy import *
from arrow_space.input.gdal_input_layer import GdalInputLayer
from gcbmwalltowall.util.gdalhelpers import *
from gcbmwalltowall.util.rasterchunks import get_memory_limited_raster_chunks
from gcbmwalltowall.util.numba import numba_map
from gcbmwalltowall.converter.layerconverter import LayerConverter

class MergingTransitionConverter(LayerConverter):
    
    def __init__(
        self,
        cbm_defaults_path: Path | str,
        sim_start_year: int,
        classifiers: list[str],
        transitions: DataFrame,
        output_path: Path | str,
        *args,
        disturbance_order: list[str] = None,
        excluded_disturbances: list[str] = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._cbm_defaults_path = Path(cbm_defaults_path)
        self._sim_start_year = sim_start_year
        self._classifiers = classifiers
        self._transitions = transitions
        self._output_path = Path(output_path)
        self._disturbance_order = disturbance_order
        self._excluded_disturbances = excluded_disturbances or []

    def handles(self, layer: PreparedLayer) -> bool:
        tags = layer.study_area_metadata.get("tags", [])
        return "disturbance" in tags and "last_pass_disturbance" not in tags

    def convert_internal(self, layers: list[PreparedLayer]) -> list[GdalInputLayer]:
        if not layers:
            return []

        logging.info(f"Merging transitions: {', '.join((l.name for l in layers))}")
        disturbance_type_order = self._load_disturbance_type_order()
        disturbance_info = [{"path": str(l.path), **l.metadata} for l in layers]
        first = disturbance_info[0]

        max_transition_id: int = self._transitions["id"].max()
        out_transition_rules = self._transitions.copy()

        years = set()
        filtered_disturbance_info = []
        for info in disturbance_info:
            has_year = False
            for _, att_value in info["attributes"].items():
                year = int(att_value["year"])
                disturbance_type = str(att_value["disturbance_type"])
                if (
                    year >= self._sim_start_year
                    and disturbance_type not in self._excluded_disturbances
                ):
                    years.add(year)
                    has_year = True
            if has_year:
                filtered_disturbance_info.append(info)

        transition_rule_ref = self._build_transition_rule_references(
            disturbance_type_order, filtered_disturbance_info, years
        )

        full_bound = get_raster_dimension(first["path"])
        chunks = list(
            get_memory_limited_raster_chunks(
                n_rasters=len(transition_rule_ref) * 3,
                width=full_bound.x_size,
                height=full_bound.y_size,
                memory_limit_MB=int(global_memory_limit / 1024**2 / max_threads),
                bytes_per_pixel=4
            )
        )

        output_raster_paths = {}
        with Manager() as manager:
            unique_composite_transition_ids = manager.dict()
            next_id = manager.Value("i", max_transition_id + 1)
            next_id_lock = manager.Lock()
            for year in years:
                out_path = self._temp_dir.joinpath(f"{year}_transition.tiff")
                output_raster_paths[year] = out_path
                create_empty_raster(
                    first["path"],
                    out_path,
                    data_type=np.int32,
                    options=gdal_creation_options,
                    nodata=0
                )
            
                with ProcessPoolExecutor() as pool:
                    tasks = []
                    for chunk in chunks:
                        tasks.append(pool.submit(
                            self._process_chunk, chunk, year, unique_composite_transition_ids,
                            next_id, next_id_lock, transition_rule_ref
                        ))
                
                    for task in as_completed(tasks):
                        chunk, raster_data, new_transitions = task.result()
                        write_output(out_path, raster_data, chunk.x_off, chunk.y_off)
                        out_transition_rules = pd.concat([out_transition_rules, new_transitions])

        out_transition_rules.to_csv(
            self._output_path.joinpath("transition.csv"), index=False
        )

        return [
            GdalInputLayer(
                f"transition_{year}",
                str(layer_path)
            ) for year, layer_path in output_raster_paths.items()
        ]
    
    def _process_chunk(
        self,
        chunk: RasterBound,
        year: int,
        unique_composite_transition_ids: dict[tuple, int],
        next_id: Value,
        next_id_lock: Lock,
        transition_rule_ref: dict
    ) -> NDArray:
        layers: dict[str, np.ndarray] = {
            path: read_dataset(path, chunk)
            .data.flatten()
            .astype("int32")
            for path in transition_rule_ref.keys()
        }
                
        output_transition_ids = np.full(
            shape=(chunk.x_size * chunk.y_size),
            fill_value=0,
            dtype="int32",
        )
                
        sorted_transition_ids, transition_indices = self._get_sorted_transition_ids(
            year, transition_rule_ref, layers, chunk
        )

        out_array = []
        new_transition_rules = pd.DataFrame()
        for row in sorted_transition_ids:
            tup = tuple(row[row > 0])
            if len(tup) == 1 or len(set(tup)) == 1:
                out_array.append(tup[0])
            else:
                if tup in unique_composite_transition_ids:
                    out_array.append(unique_composite_transition_ids[tup])
                else:
                    with next_id_lock:
                        new_id = next_id.value
                        next_id.value += 1

                    out_array.append(new_id)
                    unique_composite_transition_ids[tup] = new_id
                    new_transition_rules = self._add_composite_transition_rule(
                        new_transition_rules, tup, new_id
                    )
        
        output_transition_ids[transition_indices] = np.array(out_array)
        
        return (
            chunk,
            output_transition_ids.reshape((chunk.y_size, chunk.x_size)),
            new_transition_rules
        )

    def _load_disturbance_type_order(self) -> dict[str, int]:
        db_dist_types_df = pd.DataFrame(self._load_disturbance_types().keys(), columns=["name"])
        if self._disturbance_order:
            # ensure no duplicates in the user disturbance type order
            unique_user_dist_types = set(self._disturbance_order)
            if not len(unique_user_dist_types) == len(self._disturbance_order):
                raise ValueError(f"duplicate values detected in user disturbance type order")
            
            # check that every disturbance type in the user order exists in the database
            unknown_disturbance_types = unique_user_dist_types.difference(
                set(db_dist_types_df["name"].unique())
            )
            
            if unknown_disturbance_types:
                raise ValueError(
                    "entries in user disturbance type order not found in database"
                    f" {self._cbm_defaults_path}: {unknown_disturbance_types}"
                )
        
            output_order = self._disturbance_order
            for name in db_dist_types_df["name"]:
                if name not in unique_user_dist_types:
                    output_order.append(name)
        else:
            output_order = list(db_dist_types_df["name"])

        return {name: i for i, name in enumerate(output_order)}

    def _load_disturbance_types(self) -> dict:
        engine = create_engine(f"sqlite:///{self._cbm_defaults_path}")
        with engine.connect() as conn:
            dist_types = pd.read_sql_query(
                """
                SELECT disturbance_type_id, name
                FROM disturbance_type_tr
                WHERE locale_id = 1
                ORDER BY disturbance_type_id
                """,
                conn
            )

        return {
            str(row["name"]): int(row["disturbance_type_id"])
            for _, row in dist_types.iterrows()
        }

    def _build_transition_rule_references(
        self,    
        disturbance_type_sort: dict[str, int],
        filtered_disturbance_info: list,
        year_set: set[int]
    ):
        """
        transition_rule_ref is a nested dictionary like:
        {
          raster_path:
          {
            year:
           {
              raster_value: (transition_id, disturbance_type_sort)
           }
        }
        """
        transition_rule_ref = {}
        null_transitions = set([2])
        for year_iter in year_set:
            for info in filtered_disturbance_info:
                if info["path"] not in transition_rule_ref:
                    transition_rule_ref[info["path"]] = {}

                if year_iter not in transition_rule_ref[info["path"]]:
                    nodata = info.get("nodata", -1)
                    transition_rule_ref[info["path"]][year_iter] = {
                        nodata: (0, np.iinfo("int32").max)
                    }

                for raster_id, att_value in info["attributes"].items():
                    year = int(att_value["year"])
                    raster_id = int(raster_id)

                    if (
                        (year == year_iter)
                        and ("transition" in att_value)
                        and (int(att_value["transition"]) not in null_transitions)
                    ):
                        if int(att_value["transition"]) == 0:
                            print(info["path"])

                        transition_rule_ref[info["path"]][year][raster_id] = (
                            int(att_value["transition"]),
                            int(
                                disturbance_type_sort[
                                    att_value["disturbance_type"]
                                ]
                            ),
                        )
                    elif raster_id not in transition_rule_ref[info["path"]][year_iter]:
                        transition_rule_ref[info["path"]][year_iter][raster_id] = (
                            0, np.iinfo("int32").max
                        )

        return transition_rule_ref

    def _get_sorted_transition_ids(
        self,    
        year: int,
        transition_rule_ref: dict,
        layers: dict[str, np.ndarray],
        bound: RasterBound
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Produce a pair of numpy arrays with sort and position information for
        transtion rule ids. The specified year and bound represent the subset of
        information to process.

        Args:
            year (int): the year to process
            transition_rule_ref (dict): nested dict of transition rule id and
                transition rule sort information by file path/year
            layers (dict): dictionary of path to layer data
            bound (RasterBound): the raster bound subset to process

        Returns:
            tuple[np.ndarray, np.ndarray]: a pair of arrays::

                value_1: a 2d array whose columns are the maximum number of
                    transitions for the given year, and whose rows are the subset
                    of flattened raster positions where transition rules are
                    present. The values are transtion rule ids, sorted from left
                    to right by the transition rule sort information
                value_2: the flattened indices (1d array) of each row of value_1
                    within the raster bound.
        """
        year_transition_ids = np.full(
            shape=(
                bound.x_size * bound.y_size,
                len(transition_rule_ref),
            ),
            fill_value=0,
            dtype="int32"
        )
        
        year_transition_id_sort = np.full(
            shape=(
                bound.x_size * bound.y_size,
                len(transition_rule_ref),
            ),
            fill_value=np.iinfo("int32").max,
            dtype="int32"
        )

        for col_idx, (path, year_raster_map) in enumerate(
            transition_rule_ref.items()
        ):
            if year in year_raster_map:
                transition_id_raster_map = {
                    np.int32(k): np.int32(v[0])
                    for k, v in year_raster_map[year].items()
                }
                
                transition_sort_raster_map = {
                    np.int32(k): np.int32(v[1])
                    for k, v in year_raster_map[year].items()
                }
                
                tr_data = layers[path]
                year_transition_ids[:, col_idx] = numba_map(
                    tr_data,
                    transition_id_raster_map
                )
                
                year_transition_id_sort[:, col_idx] = numba_map(
                    tr_data,
                    transition_sort_raster_map
                )

        transition_indices = np.nonzero(np.any(year_transition_ids, axis=1))[0]
        transition_sort_arg = np.argsort(
            year_transition_id_sort[transition_indices, :], axis=1
        )
        
        sorted_transition_ids = np.take_along_axis(
            year_transition_ids[transition_indices, :],
            transition_sort_arg,
            axis=1
        )

        return sorted_transition_ids, transition_indices

    def _add_composite_transition_rule(
        self, out_transition_rules, tup, next_id
    ):
        tr_merge_subset = self._transitions.set_index("id").loc[list(tup)]
        regen_delay = int(tr_merge_subset["regen_delay"].iloc[-1])
        age_after = -1
        if (tr_merge_subset["age_after"] > -1).any():
            age_after = (
                tr_merge_subset["age_after"]
                .loc[tr_merge_subset["age_after"] > -1]
                .iloc[-1]
            )
    
        classifiers = {}
        for classifier_name in self._classifiers:
            out_value = "?"
            classifier_series = tr_merge_subset[classifier_name]
            if (classifier_series != "?").any():
                out_value = classifier_series[classifier_series != "?"].iloc[-1]
        
            classifiers[classifier_name] = out_value
    
        new_tr_data = {}
        new_tr_data["id"] = next_id
        new_tr_data["regen_delay"] = regen_delay
        new_tr_data["age_after"] = age_after
        new_tr_data.update(classifiers)
        out_transition_rules = pd.concat(
            [out_transition_rules, pd.DataFrame([new_tr_data])]
        )

        return out_transition_rules
