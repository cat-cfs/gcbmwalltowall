from __future__ import annotations
from pathlib import Path
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from gcbmwalltowall.application.command.argbase import ArgBase
from gcbmwalltowall.application.command.clone import clone


@dataclass
class ExtendArgs(ArgBase):
    cbm4_config_path: str
    disturbance_config_path: str
    output_path: str
    use_cache: bool

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            cbm4_config_path=d["cbm4_config_path"],
            disturbance_config_path=d["disturbance_config_path"],
            output_path=d.get("output_path", None),
            use_cache=d.get("use_cache", True),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            cbm4_config_path=ns.cbm4_config_path,
            disturbance_config_path=ns.disturbance_config_path,
            output_path=getattr(ns, "output_path", None),
            use_cache=getattr(ns, "use_cache", True),
        )


def extend(args: ExtendArgs | dict):
    from gcbmwalltowall.application.command.impl.cbm4project import CBM4Project
    from gcbmwalltowall.application.command.impl.disturbanceextender import (
        DisturbanceExtender,
    )

    args = args if isinstance(args, ExtendArgs) else ExtendArgs.from_dict(args)
    cbm4_config_path = args.cbm4_config_path
    if args.output_path:
        clone(
            {
                "config_path": str(cbm4_config_path),
                "output_path": str(args.output_path),
                "include_disturbances": True,
                "use_cache": args.use_cache,
            }
        )

        cbm4_config_path = Path(args.output_path).joinpath("cbm4_config.json")

    cbm4_project = CBM4Project(cbm4_config_path)
    disturbance_extender = DisturbanceExtender(cbm4_project, args.use_cache)
    disturbance_extender.add_from_walltowall_config(args.disturbance_config_path)
