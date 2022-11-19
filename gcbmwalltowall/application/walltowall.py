import logging
import sys
from psutil import virtual_memory
from pathlib import Path
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from spatial_inventory_rollback.gcbm.merge import gcbm_merge
from spatial_inventory_rollback.gcbm.merge import gcbm_merge_tile
from spatial_inventory_rollback.gcbm.merge.gcbm_merge_input_db import replace_direct_attached_transition_rules
from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.configuration.gcbmconfigurer import GCBMConfigurer
from gcbmwalltowall.component.project import Project
from gcbmwalltowall.component.preparedproject import PreparedProject

def build(args):
    ProjectBuilder.build_from_file(args.config_path, args.output_path)

def prepare(args):
    config = Configuration.load(args.config_path, args.output_path)
    project = Project.from_configuration(config)
    project.tile()
    project.create_input_database(config.recliner2gcbm_exe)
    project.run_rollback(config.recliner2gcbm_exe)

    extra_args = {
        param: config.get(param) for param in ("start_year", "end_year")
        if config.get(param)
    }

    project.configure_gcbm(config.gcbm_template_path,
                           config.gcbm_disturbance_order,
                           **extra_args)

def merge(args):
    with TemporaryDirectory() as tmp:
        projects = [PreparedProject(path) for path in args.project_paths]
        inventories = [project.prepare_merge(tmp, i) for i, project in enumerate(projects)]

        output_path = Path(args.output_path)
        merged_output_path = output_path.joinpath("layers", "merged")
        tiled_output_path = output_path.joinpath("layers", "tiled")
        db_output_path = output_path.joinpath("input_database")
        
        start_year = min((project.start_year for project in projects))
        end_year = max((project.end_year for project in projects))

        memory_limit = virtual_memory().available * 0.75 // 1024**2
        merged_data = gcbm_merge.merge(
            inventories, str(merged_output_path), str(db_output_path),
            start_year, memory_limit_MB=memory_limit)

        gcbm_merge_tile.tile(str(tiled_output_path), merged_data, inventories)
        replace_direct_attached_transition_rules(
            str(db_output_path.joinpath("gcbm_input.db")),
            str(tiled_output_path.joinpath("transition_rules.csv")))

        config = Configuration.load(args.config_path, args.output_path)
        configurer = GCBMConfigurer(
            [str(tiled_output_path)], config.gcbm_template_path,
            str(db_output_path.joinpath("gcbm_input.db")),
            str(output_path.joinpath("gcbm_project")), start_year, end_year,
            config.gcbm_disturbance_order)
    
        configurer.configure()

def run(args):
    print(args)

def cli():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = ArgumentParser(description="Manage GCBM wall-to-wall projects")
    parser.set_defaults(func=lambda _: parser.print_help())
    subparsers = parser.add_subparsers(help="Command to run")
    
    build_parser = subparsers.add_parser(
        "build",
        help=("Use the builder configuration contained in the config file to fill in and "
              "configure the rest of the project; overwrites the existing json config file "
              "unless output config file path is specified."))
    build_parser.set_defaults(func=build)
    build_parser.add_argument(
        "config_path",
        help="path to config file containing shortcut 'builder' section")
    build_parser.add_argument(
        "output_path", nargs="?",
        help="destination directory for build output")

    prepare_parser = subparsers.add_parser(
        "prepare",
        help=("Using the project configuration in the config file, tile the spatial "
              "layers, generate the input database, run the spatial rollback if "
              "specified, and configure the GCBM run."))
    prepare_parser.set_defaults(func=prepare)
    prepare_parser.add_argument(
        "config_path",
        help="path to config file containing fully-specified project configuration")
    prepare_parser.add_argument(
        "output_path", nargs="?",
        help="destination directory for project files")

    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge two or more walltowall-prepared inventories together.")
    merge_parser.set_defaults(func=merge)
    merge_parser.add_argument(
        "config_path",
        help="path to walltowall config file for disturbance order and GCBM config templates")
    merge_parser.add_argument(
        "project_paths", nargs="+",
        help="root directories of at least two walltowall-prepared projects")
    merge_parser.add_argument(
        "--output_path", required=True,
        help="path to generate merged output in")

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster.")
    run_parser.set_defaults(func=run)
    run_parser.add_argument(
        "host", choices=["local", "cluster"], help="run either locally or on the cluster")

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    cli()
