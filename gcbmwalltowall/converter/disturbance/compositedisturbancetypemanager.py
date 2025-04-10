from __future__ import annotations
import functools
import sqlite3
import pandas as pd
import numpy as np
from contextlib import closing
from typing import Sequence
from scipy.sparse import coo_matrix


class CompositeDisturbanceTypeManager:
    DISTURBANCE_TYPE_QUERY = """
        SELECT
            disturbance_type.id,
            disturbance_type_tr.name,
            disturbance_type_tr.description
        FROM disturbance_type
        INNER JOIN disturbance_type_tr
            ON disturbance_type.id = disturbance_type_tr.disturbance_type_id
        WHERE disturbance_type_tr.locale_id = ?
    """

    COMPOSITE_DIST_TYPE_QUERY = "SELECT * FROM composite_disturbance_types"
    DM_ASSOCIATION_QUERY = "SELECT * FROM disturbance_matrix_association"
    MAX_DISTURBANCE_TYPE_ID_QUERY = "SELECT MAX(id) AS id FROM disturbance_type"
    MAX_DISTURBANCE_TYPE_TR_ID_QUERY = "SELECT MAX(id) AS id FROM disturbance_type_tr"
    MAX_DISTURBANCE_MATRIX_TR_ID_QUERY = "SELECT MAX(id) AS id FROM disturbance_matrix_tr"
    MAX_DMID_QUERY = "SELECT MAX(id) AS id FROM disturbance_matrix"
    POOL_QUERY = "SELECT * FROM pool"
    DM_VALUE_QUERY = "SELECT * FROM disturbance_matrix_value"

    def __init__(self, db_path: str, locale_id: int = 1):
        self._db_path = db_path
        self._locale_id = locale_id

        # create table composite_disturbance_types if it does not exist
        if self._table_exists(db_path, "composite_disturbance_types"):
            self._composite_disturbance_types = self._to_composite_types_tuples(
                self._read_sql(db_path, self.COMPOSITE_DIST_TYPE_QUERY)
            )
        else:
            self._composite_disturbance_types = {}

        self._disturbance_types = self._read_sql(
            self._db_path, self.DISTURBANCE_TYPE_QUERY, (locale_id,)
        )
        
        # exclude composite types
        self._disturbance_types = self._disturbance_types[
            ~self._disturbance_types.id.isin(
                list(self._composite_disturbance_types.keys())
            )
        ]
        
        self._disturbance_matrix_association = self._read_sql(db_path, self.DM_ASSOCIATION_QUERY)
        
        self._spatial_units = list(
            self._disturbance_matrix_association.spatial_unit_id.unique()
        )
        
        self._dense_matrix_lookup = {}
        self._extra_source_pools = None
        self._dm_lookup = self._create_dm_association_lookup()
        self._init_dense_matrix_lookup()

    def get_all_composite_types(self) -> dict[int, tuple]:
        return self._composite_disturbance_types.copy()

    def get_max_dist_type_id(self) -> int:
        return int(self._read_sql(self._db_path, self.MAX_DISTURBANCE_TYPE_ID_QUERY).id.iloc[0])

    def add_composite_type(
        self, disturbance_type_id: int, composite_type_ids: tuple[int]
    ):
        if disturbance_type_id in self._composite_disturbance_types:
            # if the disturbance type id/composite already exists in the db, do nothing
            if composite_type_ids == self._composite_disturbance_types[disturbance_type_id]:
                return

            raise ValueError(
                f"specified disturbance type id {disturbance_type_id} "
                "already exists and does not match an existsing composite "
                "type"
            )

        # collect the names of the member composite type
        composites_df = self._to_composite_types_df(
            {disturbance_type_id: composite_type_ids}
        )
        
        dist_type_composite_merge = composites_df.merge(
            self._disturbance_types,
            left_on="disturbance_type_id",
            right_on="id"
        )
        
        if len(dist_type_composite_merge.index) != len(composite_type_ids):
            missing = set(composite_type_ids) - set(
                dist_type_composite_merge["disturbance_type_id"]
            )
            
            raise ValueError(
                "specified values in composite are not defined disturbance "
                f"types {missing}"
            )

        # insert record to disturbance_type, disturbance_type_tr
        self._to_sql(
            self._db_path,
            "disturbance_type",
            pd.DataFrame(
                {"id": [disturbance_type_id], "land_type_id": None}
            ),
            if_exists="append"
        )
        
        self._to_sql(
            self._db_path,
            "disturbance_type_tr",
            pd.DataFrame(
                {
                    "id": [
                        self._read_sql(
                            self._db_path,
                            self.MAX_DISTURBANCE_TYPE_TR_ID_QUERY
                        ).id.iloc[0]
                        + 1
                    ],
                    "disturbance_type_id": [disturbance_type_id],
                    "locale_id": [self._locale_id],
                    "name": [f"composite ({composite_type_ids})"],
                    "description": [f"composite ({composite_type_ids})"]
                }
            ),
            if_exists="append"
        )
        
        # insert one record with the disturbance_type_id, composite_dist_type
        # for each composite_dist_type in the tuple
        self._composite_disturbance_types[
            disturbance_type_id
        ] = composite_type_ids
        
        self._to_sql(
            self._db_path,
            "composite_disturbance_types",
            pd.DataFrame(
                {
                    "composite_disturbance_type_id": disturbance_type_id,
                    "disturbance_type_id": list(composite_type_ids)
                }
            ),
            if_exists="append"
        )
        
        self._process_dm(disturbance_type_id, composite_type_ids)

    def _process_dm(
        self, disturbance_type_id: int, composite_type_ids: tuple[int]
    ):
        dmid = (
            int(
                self._read_sql(
                    self._db_path,
                    self.MAX_DMID_QUERY
                ).id.iloc[0]
            )
            + 1
        )
        
        dm_tr_id = (
            int(
                self._read_sql(
                    self._db_path,
                    self.MAX_DISTURBANCE_MATRIX_TR_ID_QUERY
                ).id.iloc[0]
            )
            + 1
        )
        
        dm_association_insertions = []
        dm_insertions = []
        dm_tr_insertions = []
        dm_value_insertions = None
        for spatial_unit in self._spatial_units:
            spu_dm_lookup = self._dm_lookup[spatial_unit]
            dm_list = []
            missing_dms = False
            for composite_type_id in composite_type_ids:
                if composite_type_id in spu_dm_lookup:
                    dm_list.append(spu_dm_lookup[composite_type_id])
                else:
                    missing_dms = True
                    break

            if missing_dms:
                continue
            
            dm_insertions.append({"id": dmid})

            dm_association_insertions.append(
                {
                    "spatial_unit_id": spatial_unit,
                    "disturbance_type_id": disturbance_type_id,
                    "disturbance_matrix_id": dmid
                }
            )

            dm_tr_insertions.append(
                {
                    "id": dm_tr_id,
                    "disturbance_matrix_id": dmid,
                    "locale_id": self._locale_id,
                    "name": f"composite dm {dm_list}",
                    "description": f"composite dm {dm_list}"
                }
            )

            dm_value_insertions = pd.concat(
                [
                    dm_value_insertions,
                    self._create_composite_dm_insertions(dmid, dm_list)
                ],
                ignore_index=True
            )
            
            dm_tr_id += 1
            dmid += 1

        self._to_sql(
            self._db_path,
            "disturbance_matrix",
            pd.DataFrame(dm_insertions),
            if_exists="append"
        )

        self._to_sql(
            self._db_path,
            "disturbance_matrix_value",
            dm_value_insertions,
            if_exists="append"
        )
        
        self._to_sql(
            self._db_path,
            "disturbance_matrix_tr",
            pd.DataFrame(dm_tr_insertions),
            if_exists="append"
        )

        self._to_sql(
            self._db_path,
            "disturbance_matrix_association",
            pd.DataFrame(dm_association_insertions),
            if_exists="append"
        )

    def _create_dm_association_lookup(self) -> dict:
        dm_association_lookup = {}
        for _, row in self._disturbance_matrix_association.iterrows():
            spatial_unit_id = int(row["spatial_unit_id"])
            disturbance_type_id = int(row["disturbance_type_id"])
            disturbance_matrix_id = int(row["disturbance_matrix_id"])
            if spatial_unit_id in dm_association_lookup:
                dm_association_lookup[spatial_unit_id][disturbance_type_id] = disturbance_matrix_id
            else:
                dm_association_lookup[spatial_unit_id] = {
                    disturbance_type_id: disturbance_matrix_id
                }
        
        return dm_association_lookup

    def _init_dense_matrix_lookup(self):
        pools = self._read_sql(self._db_path, self.POOL_QUERY)
        raw_dm_values = self._read_sql(self._db_path, self.DM_VALUE_QUERY)

        dmids = raw_dm_values.disturbance_matrix_id.unique()
        for dmid in dmids:
            dm_values = raw_dm_values[
                raw_dm_values.disturbance_matrix_id == dmid
            ]
            
            missing_sources = pools[~pools.id.isin(dm_values.source_pool_id)]
            dm_values = pd.concat(
                [
                    dm_values,
                    pd.DataFrame(
                        [
                            {
                                "disturbance_matrix_id": dmid,
                                "source_pool_id": missing,
                                "sink_pool_id": missing,
                                "proportion": 1.0,
                            }
                            for missing in missing_sources.id
                        ]
                    ),
                ]
            )
            
            dense_mat = coo_matrix(
                (
                    dm_values.proportion,
                    (dm_values.source_pool_id, dm_values.sink_pool_id),
                )
            ).toarray()
            
            self._dense_matrix_lookup[dmid] = dense_mat
            
            # by convention CBM3 matrices exclude rows
            # corresponding to emission sources, which are
            # actually needed to construct a complete matrix
            extras = set([int(i) for i in missing_sources.id])
            if not self._extra_source_pools:
                self._extra_source_pools = extras
            elif extras != self._extra_source_pools:
                raise ValueError("emission pools mismatch")

    def _create_composite_dm_insertions(
        self, dmid: int, dm_list: list[int]
    ) -> pd.DataFrame:
        mats = [self._dense_matrix_lookup[d] for d in dm_list]
        composite_matrix_dense = functools.reduce(np.matmul, mats)
        composite_coo_mat = coo_matrix(composite_matrix_dense)
        result = pd.DataFrame(
            {
                "disturbance_matrix_id": dmid,
                "source_pool_id": composite_coo_mat.row,
                "sink_pool_id": composite_coo_mat.col,
                "proportion": composite_coo_mat.data
            }
        )
        result = result[~result.source_pool_id.isin(self._extra_source_pools)]
        return result

    def _read_sql(self, db_path: str, sql: str, params: Sequence[str] = None):
        with closing(sqlite3.connect(db_path)) as con:
            df = pd.read_sql(sql, con, params=params)
            return df

    def _to_sql(self, db_path: str, table_name: str, df: pd.DataFrame, if_exists: str):
        with closing(sqlite3.connect(db_path)) as con:
            df = df.to_sql(table_name, con, index=False, if_exists=if_exists)
            return df

    def _table_exists(self, db_path: str, table_name: str) -> bool:
        result = self._read_sql(
            db_path,
            "SELECT name FROM sqlite_master " "WHERE type='table' AND name=?",
            params=[table_name]
        )
    
        return len(result.index) > 0

    def _to_composite_types_df(self, composite_types: dict[int, tuple]) -> pd.DataFrame:
        rows = {"composite_disturbance_type_id": [], "disturbance_type_id": []}
        for k, v in composite_types.items():
            for composite_value in v:
                rows["composite_disturbance_type_id"].append(k)
                rows["disturbance_type_id"].append(composite_value)
     
        return pd.DataFrame(rows)

    def _to_composite_types_tuples(self, composite_types_df: pd.DataFrame) -> dict[int, tuple]:
        result = {}
        ids = composite_types_df["composite_disturbance_type_id"].unique()
        for composite_disturbance_type_id in ids:
            result[int(composite_disturbance_type_id)] = tuple(
                [
                    int(x)
                    for x in composite_types_df.loc[
                        composite_types_df.composite_disturbance_type_id == composite_disturbance_type_id,
                        "disturbance_type_id"
                    ]
                ]
            )
    
        return result
