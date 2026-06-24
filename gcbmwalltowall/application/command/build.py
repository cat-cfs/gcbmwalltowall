from __future__ import annotations
import logging
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.application.command.argbase import ArgBase


@dataclass
class BuildArgs(ArgBase):
    config_path: str
    output_path: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            config_path=d["config_path"],
            output_path=d["output_path"],
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            config_path=ns.config_path,
            output_path=ns.output_path,
        )


def build(args: BuildArgs | dict):
    args = args if isinstance(args, BuildArgs) else BuildArgs.from_dict(args)
    logging.info(f"Building {args.config_path}")
    ProjectBuilder.build_from_file(args.config_path, args.output_path)
