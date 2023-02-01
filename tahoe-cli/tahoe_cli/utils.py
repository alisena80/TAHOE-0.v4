import click
import os
import shutil
from typing import List
import json
import string

def find_tahoe_root():
    """
    Finds root of tahoe directory.

    Searches in current install site of cli.
    
    if the /tahoe/ directory is not found supply TAHOE_ROOT_LOCATION as location of tahoe repository. 
    TAHOE_ROOT_LOCATION is overrided if tahoe directory is found in current install sit eof cli.
    """
    tahoe_location = __file__.rfind(f"{os.sep}tahoe{os.sep}")
    if tahoe_location and "TAHOE_ROOT_LOCATION" not in os.environ:
        os.environ["TAHOE_ROOT_LOCATION"] = __file__[0:tahoe_location + len(f"{os.sep}tahoe{os.sep}")]
    if "TAHOE_ROOT_LOCATION" not in os.environ:
        click.echo("Could not find tahoe directory set TAHOE_ROOT_LOCATION env")
    # look for os env variable to get location of tahoe directory
    return os.environ["TAHOE_ROOT_LOCATION"]

def create_folder(path: str):
    """
    create folder

    :param path: Path to create folder in
    """
    click.echo(f"CREATING FOLDER: {path}")
    os.mkdir(path)


def delete_folder(path: str, prompt: bool):
    """
    delete folder

    :param path: path to delete
    :param prompt: verify from cli
    """
    confirm = True
    if prompt:
        confirm = click.confirm(f"Confirm deleting {path}")
    if confirm:
        click.echo(f"DELETING FOLDER: {path}")
        shutil.rmtree(path)


def create_hierarchy(infra_path: str, path: str, name: str, folders: List[str]):
    """
    Create folder hierarchy

    :param infra_path: infrastructure path 
    :param path: etl code path
    :param name: name of source
    :param folders: folders to create in etl path
    """
    create_folder(os.path.join(path, name))
    create_folder(os.path.join(infra_path, name))
    for folder in folders:
        create_folder(os.path.join(path, name, folder))


def undo_hierarchy(infra_path: str, path: str, name: str, prompt: bool):
    """
    Delete heirarchy

    :param infra_path: infrastructure path 
    :param path: etl code path
    :param name: name of source
    :param prompt: ask before attempting to delete folders
    """
    try:
        delete_folder(os.path.join(path, name), prompt)
    except FileNotFoundError:
        click.echo(f"Could not find {os.path.join(path, name)}")

    try:
        delete_folder(os.path.join(infra_path, name), prompt)
    except FileNotFoundError:
        click.echo(f"could not find {os.path.join(infra_path, name)}")


def update_datasources_base_stack(ctx, actor: str, name: str):
    """
    Updates the base stack datasource.py file with new datasource

    :param ctx: cli context
    :param name: name of datasource
    """
    lines = None
    with open(ctx.obj[f"{actor}_nested_path"], "r") as fh:
        lines = fh.readlines()
        deployable = f'if self.can_deploy("{name}")'
        lines.insert(0, '''from stacks.{actor}s.{name}.{name}_{actor}_stack import {name_capital}{actor_capital}InfrastructureStack\n'''.format(
            actor=actor, actor_capital=actor.capitalize(), name=name, name_capital=capitalize(name)))
        lines.append('''
        # {name} {actor}
        self.{name}_actor = None
        {deployable}:
            self.{name}_{actor} = {name_capital}{actor_capital}InfrastructureStack(
                self, "{name}", data_prefix="{name}", build_context=self.build_context, base_context=base_context, log=self.can_log("{name}"))    
        '''.format(actor=actor, actor_capital=actor.capitalize(), name=name, name_capital=capitalize(name), deployable=deployable))
    with open(ctx.obj[f"{actor}_nested_path"], "w") as fh:
        fh.writelines(lines)


def update_consumer_base_stack(ctx, actor: str, name: str, depends_on: str = None):
    """
    Updates the base stack datasource.py file with new datasource

    :param ctx: cli context
    :param name: name of datasource
    :param depends_on: datasource dependencies
    """
    lines = None
    with open(ctx.obj[f"{actor}_nested_path"], "r") as fh:
        lines = fh.readlines()
        deployable = ""
        if depends_on:
            deployable += ", ".join([f"datasources.{x}_datasource" for x in depends_on])

        lines.insert(0, '''from stacks.{actor}s.{name}.{name}_{actor}_stack import {name_capital}{actor_capital}InfrastructureStack\n'''.format(
            actor=actor, actor_capital=actor.capitalize(), name=name, name_capital=capitalize(name)))
        lines.append('''
        # {name} {actor}
        {name}_{actor}_dependents = DependentDatasources({deployable})
        if {name}_{actor}_dependents.deployable():
            self.{name}_{actor} = {name_capital}{actor_capital}InfrastructureStack(
                self, "{name}", data_prefix="{name}", build_context=self.build_context, base_context=base_context, datasources={name}_{actor}_dependents)    
        '''.format(actor=actor, actor_capital=actor.capitalize(), name=name, name_capital=capitalize(name), deployable=deployable))
    with open(ctx.obj[f"{actor}_nested_path"], "w") as fh:
        fh.writelines(lines)


def get_subsections(subsections: set, file: str):
    """
    Gets the current state of the file sections

    :param subsections: The subsections to transplant
    :param file: File to read current data to transpose to new file
    """
    bucket = {}
    subsection = None
    with open(file) as fh:
        for lines in fh:
            words = lines.split()
            if len(words) == 3 and words[0] == "#" and words[1] == subsection and words[2] == "END":
                subsection = None
            elif len(words) == 3 and words[0] == "#" and words[1] in subsections and words[2] == "START":
                subsection = words[1]
                if subsection not in bucket:
                    bucket[subsection] = []
            elif subsection is not None:
                bucket[subsection].append(lines)
    if bucket == {}:
        raise ValueError(f"EXISTING FILE {file} DOES NOT HAVE COMMENT ANCHORS")
    return bucket


def in_imports(imports, etl_type, data):
    """
    Verify basic imports exist

    :param imports: Import to check against defaults
    :param etl_type: PYSHELL or PYSPARK
    :param data: default imports dict
    """
    if imports not in data["DEFAULT_IMPORTS"]:
        if imports not in data[f"{etl_type}_DEFAULTS"]:
            return True
    return False


def validate_name(name):
    """
    Validate names to avoid punctuation

    :param name: name to validate
    """
    ignore_symbols = string.punctuation
    for sym in ignore_symbols:
        if sym != "_":
            if sym in name:
                raise click.UsageError("Name is not valid")


def capitalize(name: str):
    """
    format name to proper capitalization
    :param name: capatalize target
    """
    if "_" in name:
        n = name.split("_")
        return "".join([x.capitalize() for x in n])
    else:
        return name.capitalize()


def write_configruation(ctx: click.Context, configurations: dict):
    """
    Add configuration of new datasourrce to all cdk.json environments.
    :param ctx: cli context
    :param configurations: configuration json replace cdk.json file
    """
    with open(ctx.obj["cdk_json"], "w") as file:
        json.dump(configurations, file)


def read_configuration(ctx: click.Context):
    """
    Load cdk.json file

    :param ctx: cli context
    """
    with open(ctx.obj["cdk_json"], "r") as file:
        configurations = json.loads(file.read())
        return configurations


def delete_base_stack(ctx, actor: str, name: str):
    """
    Delete nested stack configuration from base stack datasource or consumer file
    :param ctx: cli context
    :param actor: wether the delete is from a datasourrce or consumer
    :param name: name of datasource 
    """
    lines = None
    with open(ctx.obj[f"{actor}_nested_path"], "r") as fh:
        lines = fh.readlines()
        try:
            lines.remove('''from stacks.{actor}s.{name}.{name}_{actor}_stack import {name_capital}{actor_capital}InfrastructureStack\n'''.format(
                name=name, name_capital=capitalize(name), actor=actor, actor_capital=capitalize(actor)))
            lines_spaces = [line.strip().strip('\n') for line in lines]
            index = lines_spaces.index("# {name} {actor}".format(name=name, actor=actor))
            lines[index] = ''
            while True:
                index += 1
                if index < len(lines_spaces) and "#" not in lines_spaces[index]:
                    lines[index] = ''
                else:
                    break
        except ValueError as e:
            click.echo(f"Could not find {actor} in {ctx.obj[f'{actor}_nested_path']}")
    with open(ctx.obj[f"{actor}_nested_path"], "w") as fh:
        fh.writelines(lines)


def write_jinja_template(ctx: click.Context, params: dict, template: str, location: str):
    """
    write template to create cdk files

    :param ctx: cli context
    :param params: jinja tempalte params
    :param template: template to use
    :patam location: location to render template

    """
    template = ctx.obj["jinja_env"].get_template(os.path.join(template))
    outputText = template.render(params)
    with open(location, "w") as fh:
        fh.write(outputText)

def get_infra_path(actor: str):
    """
    get infra path

    :param actor: datasource or consumer
    """
    return os.path.join(os.environ["TAHOE_ROOT_LOCATION"],
                        "tahoe_infrastructure", "tahoe_infrastructure", "stacks", f"{actor}s")


def get_nested_init_file_path(actor: str):
    """
    get base stack construct path

    :param actor: datasource or consumer
    """
    return os.path.join(
        os.environ["TAHOE_ROOT_LOCATION"],
        "tahoe_infrastructure", "tahoe_infrastructure", "stacks", "base", "constructs", f"{actor}s.py")

def get_configs_raw(env):
    """
    Get current envs 

    :param env: env name
    """
    return [json.load(open(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", env, x))) for x in os.listdir(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", env)) if x.split(".")[1] == "json"]