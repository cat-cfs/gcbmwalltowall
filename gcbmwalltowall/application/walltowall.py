from __future__ import annotations
import logging
import multiprocessing as mp
from argparse import ArgumentParser, Namespace
from logging import FileHandler, StreamHandler
from gcbmwalltowall.util.path import Path
from gcbmwalltowall.application.command.convert import convert, ConvertArgs
from gcbmwalltowall.application.command.build import build, BuildArgs
from gcbmwalltowall.application.command.prepare import prepare, PrepareArgs
from gcbmwalltowall.application.command.merge import merge, MergeArgs
from gcbmwalltowall.application.command.run import run, RunArgs
from gcbmwalltowall.application.command.clone import clone, CloneArgs


def _convert(args: Namespace):
    convert(ConvertArgs.from_namespace(args))


def _build(args: Namespace):
    build(BuildArgs.from_namespace(args))


def _prepare(args: Namespace):
    prepare(PrepareArgs.from_namespace(args))


def _merge(args: Namespace):
    merge(MergeArgs.from_namespace(args))


def _run(args: Namespace):
    run(RunArgs.from_namespace(args))


def _clone(args: Namespace):
    clone(CloneArgs.from_namespace(args))


def cli():
    try:
        mp.set_start_method("spawn")
    except:
        pass

    parser = ArgumentParser(description="Manage GCBM wall-to-wall projects")
    parser.set_defaults(func=lambda _: parser.print_help())
    subparsers = parser.add_subparsers(help="Command to run")

    build_parser = subparsers.add_parser(
        "build",
        help=(
            "Use the builder configuration contained in the config file to fill in and "
            "configure the rest of the project; overwrites the existing json config file "
            "unless output config file path is specified."
        ),
    )
    build_parser.set_defaults(func=_build)
    build_parser.add_argument(
        "config_path", help="path to config file containing shortcut 'builder' section"
    )
    build_parser.add_argument(
        "output_path", nargs="?", help="destination directory for build output"
    )

    prepare_parser = subparsers.add_parser(
        "prepare",
        help=(
            "Using the project configuration in the config file, tile the spatial "
            "layers, generate the input database, run the spatial rollback if "
            "specified, and configure the GCBM run."
        ),
    )
    prepare_parser.set_defaults(func=_prepare)
    prepare_parser.add_argument(
        "config_path",
        help="path to config file containing fully-specified project configuration",
    )
    prepare_parser.add_argument(
        "output_path", nargs="?", help="destination directory for project files"
    )
    prepare_parser.add_argument("--max_workers", type=int, help="max workers")
    prepare_parser.add_argument("--max_mem_gb", type=int, help="max memory (GB)")

    merge_parser = subparsers.add_parser(
        "merge", help="Merge two or more walltowall-prepared inventories together."
    )
    merge_parser.set_defaults(func=_merge, include_index_layer=False)
    merge_parser.add_argument(
        "config_path",
        help="path to walltowall config file for disturbance order and GCBM config templates",
    )
    merge_parser.add_argument(
        "project_paths",
        nargs="+",
        help="root directories of at least two walltowall-prepared projects",
    )
    merge_parser.add_argument(
        "--output_path", required=True, help="path to generate merged output in"
    )
    merge_parser.add_argument(
        "--include_index_layer",
        action="store_true",
        help="include merged index as reporting classifier",
    )
    merge_parser.add_argument("--max_mem_gb", type=int, help="max memory (GB)")

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster."
    )
    run_parser.set_defaults(func=_run)
    run_parser.add_argument(
        "host",
        choices=["local", "cluster"],
        help="run either locally or on the cluster",
    )
    run_parser.add_argument(
        "project_path", help="root directory of the walltowall-prepared project to run"
    )
    run_parser.add_argument(
        "--config_path",
        help="path to config file containing fully-specified project configuration",
    )
    run_parser.add_argument(
        "--end_year", type=int, help="temporarily set a new end year for this run"
    )
    run_parser.add_argument("--title", help="explicitly specify a title for this run")
    run_parser.add_argument(
        "--compile_results_config", help="path to custom compile results config file"
    )
    run_parser.add_argument(
        "--batch_limit", help="[cluster only] batch limit for cluster runs"
    )
    run_parser.add_argument(
        "--max_workers", type=int, help="[cbm4 only] max workers for CBM4 runs"
    )
    run_parser.add_argument(
        "--engine",
        help="[cbm4 only] (libcbm/cbmspec) specify the CBM4 engine to use; default: libcbm",
        default="libcbm",
    )
    run_parser.add_argument(
        "--write_parameters",
        action="store_true",
        help="[cbm4 only] write parameters datasets; default: false",
    )

    convert_parser = subparsers.add_parser(
        "convert", help=("Convert a walltowall-prepared GCBM project to CBM4.")
    )
    convert_parser.set_defaults(func=_convert, creation_options={})
    convert_parser.add_argument(
        "project_path", help="root directory of a walltowall-prepared GCBM project"
    )
    convert_parser.add_argument(
        "output_path", help="destination directory for CBM4 project files"
    )
    convert_parser.add_argument(
        "--aidb_path", help="AIDB to use when building CBM4 input database"
    )
    convert_parser.add_argument("--chunk_size", help="maximum CBM4 chunk size")
    convert_parser.add_argument(
        "--spinup_disturbance_type", help="override default spinup disturbance type"
    )
    convert_parser.add_argument(
        "--max_workers", type=int, help="max workers for CBM4 conversion"
    )
    convert_parser.add_argument(
        "--preserve_temp_files",
        action="store_true",
        help="preserve temporary files generated during conversion",
    )
    convert_parser.add_argument(
        "--optimize_spinup",
        action="store_true",
        help="optimize spinup by converting yields to long/gcid format",
    )
    convert_parser.add_argument(
        "--include_rollback_info",
        action="store_true",
        help="include rollback procedure info as a reporting classifier",
    )

    clone_parser = subparsers.add_parser(
        "clone", help="Clone a CBM4 project, using the original as a base/cached run."
    )
    clone_parser.set_defaults(func=_clone, creation_options={})
    clone_parser.add_argument(
        "cbm4_config_path", help="path to base CBM4 project's cbm4_config.json"
    )
    clone_parser.add_argument(
        "output_path", help="destination directory for cloned project"
    )
    clone_parser.add_argument(
        "--start_year", type=int, help="start year of cloned project, up to 1 year past the end of the base project"
    )
    clone_parser.add_argument(
        "--end_year", type=int, help="end year of cloned project"
    )
    clone_parser.add_argument(
        "--include_disturbances",
        action="store_true",
        help="include disturbance data from base project",
    )

    args = parser.parse_args()

    log_path = Path(
        args.output_path
        if getattr(args, "output_path", None)
        else args.project_path if getattr(args, "project_path", None) else "."
    ).joinpath("walltowall.log")

    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            FileHandler(log_path, mode=("a" if args.func == run else "w")),
            StreamHandler(),
        ],
    )

    args.func(args)


if __name__ == "__main__":
    cli()
