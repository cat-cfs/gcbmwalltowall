import logging
import sys
from argparse import ArgumentParser
from gcbmwalltowall.builder.projectbuilder import ProjectBuilder
from gcbmwalltowall.configuration.configuration import Configuration
from gcbmwalltowall.component.project import Project

def build(args):
    ProjectBuilder.build_from_file(args.config_path, args.output_path)

def prepare(args):
    config = Configuration.load(args.config_path, args.output_path)
    project = Project.from_configuration(config)
    project.tile()
    project.create_input_database(config.recliner2gcbm_exe)

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

    run_parser = subparsers.add_parser(
        "run", help="Run the specified project either locally or on the cluster.")
    run_parser.add_argument(
        "host", choices=["local", "cluster"], help="run either locally or on the cluster")
    run_parser.set_defaults(func=run)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    cli()
