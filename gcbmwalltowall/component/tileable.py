from __future__ import annotations

from typing import Any


class Tileable:

    def __init__(self):
        raise RuntimeError("Interface only")

    def to_tiler_layer(self, rule_manager: TransitionRuleManager, **kwargs: Any) -> Any:
        raise NotImplementedError()
