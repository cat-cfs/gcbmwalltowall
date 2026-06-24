from __future__ import annotations
import logging
from argparse import Namespace
from dataclasses import dataclass
from typing import Any
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.project.projectfactory import ProjectFactory
from gcbmwalltowall.application.command.argbase import ArgBase


@dataclass
class PrepareArgs(ArgBase):
    config_path: str
    output_path: str
    max_workers: int
    max_mem_gb: int

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            config_path=d["config_path"],
            output_path=d.get("output_path", None),
            max_workers=d.get("max_workers", None),
            max_mem_gb=d.get("max_mem_gb", None),
        )

    @classmethod
    def from_namespace(cls, ns: Namespace):
        return cls(
            config_path=ns.config_path,
            output_path=getattr(ns, "output_path", None),
            max_workers=getattr(ns, "max_workers", None),
            max_mem_gb=getattr(ns, "max_mem_gb", None),
        )


def prepare(args: PrepareArgs | dict):
    args = args if isinstance(args, PrepareArgs) else PrepareArgs.from_dict(args)
    config = Configuration.load(args.config_path, args.output_path)
    config["max_workers"] = args.max_workers
    config["max_mem_gb"] = args.max_mem_gb
    project = ProjectFactory().create(config)
    logging.info(f"Preparing {project.name}")

    project.tile()
    project.create_input_database()
    project.run_rollback()

    extra_args = {
        param: config.get(param)
        for param in ("start_year", "end_year")
        if config.get(param)
    }

    project.configure_gcbm(
        config.gcbm_template_path, config.gcbm_disturbance_order, **extra_args
    )
