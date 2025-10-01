from __future__ import annotations

from numbers import Number
from typing import Any


class AttributeTable:

    def __init__(self):
        raise RuntimeError("Interface only")

    @property
    def attributes(self) -> list[str]:
        raise NotImplementedError()

    def to_tiler_args(
        self,
        attributes: str | list[str] = None,
        filters: dict[str, Any | list[Any]] = None,
    ) -> dict[str, Any]:
        raise NotImplementedError()

    def get_unique_values(
        self, attributes: str | list[str] = None
    ) -> dict[str, list[Any]]:
        raise NotImplementedError()

    def is_numeric(self, attribute: str) -> bool:
        return all(
            (
                isinstance(v, Number)
                for v in self.get_unique_values(attribute)[attribute]
            )
        )
