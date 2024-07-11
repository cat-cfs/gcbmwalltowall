from __future__ import annotations
from gcbmwalltowall.util.rasterbound import RasterBound
import numpy as np


class EventMerger:
    def __init__(
        self,
        stack_bound: RasterBound,
        dist_type_sort: dict[int, int],
        composite_dist_type_id_base: int,
        keep_duplicates: bool = False,
        composite_dist_types: dict[int, tuple] = None,
    ):
        self._dist_type_sort = dist_type_sort
        self._dist_type_id_base = composite_dist_type_id_base
        self._keep_duplicates = keep_duplicates
        self._merged_dist_types = np.full(
            shape=(stack_bound.y_size, stack_bound.x_size),
            fill_value=0,
            dtype=np.int32,
        )

        if not composite_dist_types:
            composite_dist_types = {}
        self._composite_dist_types_by_type: dict[
            int, tuple
        ] = composite_dist_types
        self._composite_dist_types = {}
        for k, v in composite_dist_types.items():
            if v in self._composite_dist_types:
                raise ValueError(f"duplicate composite dist type detected {v}")
            else:
                self._composite_dist_types[v] = k

    @property
    def composite_dist_types_by_type(self) -> dict[int, tuple]:
        return self._composite_dist_types_by_type

    @property
    def composite_dist_types(self) -> dict[tuple, int]:
        return self._composite_dist_types

    @property
    def merged_layer(self) -> np.ndarray:
        return self._merged_dist_types

    def _get_dist_type_list(self, x):
        disturbance_type_list = []
        if x in self._composite_dist_types_by_type:
            disturbance_type_list.extend(self._composite_dist_types_by_type[x])
        else:
            disturbance_type_list.append(x)
        return disturbance_type_list

    def _get_or_add_composite_type(self, lh: int, rh: int):
        dist_types = self._get_dist_type_list(lh) + self._get_dist_type_list(
            rh
        )
        if not self._keep_duplicates:
            dist_types = set(dist_types)
        dist_types = tuple(
            sorted(dist_types, key=self._dist_type_sort.__getitem__)
        )
        match = self._composite_dist_types.get(dist_types)
        if match:
            return match
        else:
            new_type = self._dist_type_id_base
            self._dist_type_id_base += 1
            self._composite_dist_types[dist_types] = new_type
            self._composite_dist_types_by_type[new_type] = dist_types

            return new_type

    def _merge_arrays(self, lh: np.ndarray, rh: np.ndarray):
        shape = lh.shape
        paired = np.column_stack([lh.flatten(), rh.flatten()])
        output = []
        for row in range(paired.shape[0]):
            composite_type_id = self._get_or_add_composite_type(
                int(paired[row][0]), int(paired[row][1])
            )
            output.append(composite_type_id)
        return np.array(output).reshape(shape)

    def merge(self, bound: RasterBound, data: np.ndarray):
        # slice the merged array according to the bounds of the incoming data
        sliced_merged = self._merged_dist_types[
            bound.y_off : bound.y_off + bound.y_size,  # noqa 501
            bound.x_off : bound.x_off + bound.x_size,  # noqa 501
        ]

        # assign the merged layer values where it is currently nodata (0) and
        # the incoming value is nonzero
        assignment_loc = (sliced_merged == 0) & (data != 0)

        # where both the merged layer and incoming data are defined, we need
        # to merge the disturbance types
        merge_loc = (sliced_merged != 0) & (data != 0)
        if not self._keep_duplicates:
            merge_loc = merge_loc & ~(sliced_merged == data)
        merge_lh = sliced_merged[merge_loc]
        merge_rh = data[merge_loc]
        merged = self._merge_arrays(merge_lh, merge_rh)

        sliced_merged[assignment_loc] = data[assignment_loc]
        sliced_merged[merge_loc] = merged

        self._merged_dist_types[
            bound.y_off : bound.y_off + bound.y_size,  # noqa 501
            bound.x_off : bound.x_off + bound.x_size,  # noqa 501
        ] = sliced_merged
