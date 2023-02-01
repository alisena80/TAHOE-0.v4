import jinja2
import os
import click
import string
import json
from tahoe_cli.config_model import ConfigModel, Datasources
from tahoe_cli.utils import *


def get_configs(env, prompt=False, optional=False):
    """
    Get current envs 

    :param env: env name
    """
    models = []
    for file in os.listdir(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", env)):
        if file.split(".")[1] == "json":
            if prompt:
                click.echo(f"VERIFYING {file}")
            model = ConfigModel(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", env), json.load(
                open(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", env, file))), prompt, optional)
            if model.was_prompted():
                model.write()
            models.append(model)
    return models


@click.group()
@click.pass_context
def cli(ctx: click.Context):
    """
    Cli entrypoint create context dict.

    :param ctx: cli context
    """
    ctx.obj = {}
    find_tahoe_root()
    ctx.obj["datasources_path"] = os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "datasources")
    ctx.obj["consumer_path"] = os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "consumers")
    ctx.obj["datasource_infra_path"] = get_infra_path("datasource")
    ctx.obj["consumer_infra_path"] = get_infra_path("consumer")
    ctx.obj["datasource_nested_path"] = get_nested_init_file_path("datasource")
    ctx.obj["consumer_nested_path"] = get_nested_init_file_path("consumer")
    ctx.obj["cdk_json"] = os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", "cdk.json")
    templateLoader = jinja2.FileSystemLoader(searchpath=os.path.join(os.path.dirname(__file__), 'templates'))
    ctx.obj["jinja_env"] = jinja2.Environment(
        loader=templateLoader, trim_blocks=True, lstrip_blocks=True, autoescape=True)
    ctx.obj["default_pyspark_imports"] = sorted({"from pyspark.context import SparkContext\n",
                                                 "from awsglue.context import GlueContext\n",
                                                 "from awsglue.job import Job\n"})
    ctx.obj["default_imports"] = sorted({"import sys\n",
                                         "from tahoelogger import log\n",
                                         "from awsglue.utils import getResolvedOptions\n"})
    ctx.obj["default_pyshell_imports"] = sorted({"from tahoecommon import commonutil\n"})
    pass


@cli.command()
@click.option("-n", "--name", type=str, required=True, help="Datasource name")
@click.option("-p", "--prompt",
              is_flag=True, show_default=True, default=False, help="Prompt to remove each folder")
@click.pass_context
def remove_datasource(ctx: click.Context, name: str, prompt: str):
    """
    Remove a datasource.

    :param ctx: cli context
    :param name: datasource name
    :param prompt: ask before deleting
    """
    try:
        c_env = get_configs("custom_env")
        r_env = get_configs("required_env")
    except:
        ValueError("Failed to load existing configs run verify-existing-configs enable missing configuration")
    check = click.confirm(f"Confirm delete of datasource {name}. FILES WILL BE UNRECOVERABLE. ")
    if check:
        undo_hierarchy(ctx.obj["datasource_infra_path"], ctx.obj["datasources_path"], name, prompt)
        # remove datasource from cdk json
        configs: ConfigModel
        for configs in c_env:
            datasource: Datasources = configs.datasources.get_value()
            datasource.remove_datasource(name)
            configs.write()
        for configs in r_env:
            datasource: Datasources = configs.datasources.get_value()
            datasource.remove_datasource(name)
            configs.write()
        delete_base_stack(ctx, "datasource", name)


@cli.command()
@click.option("-p", "--prompt",
              is_flag=True, show_default=True, default=False, help="Prompt to create new config")
@click.option("-o", "--check_optional", is_flag=True, show_default=True, default=True, help="Prompt for optional")
@click.pass_context
def verify_existing_configs(ctx: click.Context, prompt: bool, check_optional: bool):
    """
    Verify all existing configs.

    :param ctx: cli context
    :param prompt: ask for value if missing fields
    :param check_optional: ask for value of optional fields

    """
    get_configs("custom_env", prompt, check_optional)
    get_configs("required_env", prompt, check_optional)
    click.echo("CONFIGS HAVE BEEN VERIFIED")


@cli.command()
@click.option("-p", "--prompt",
              is_flag=True, show_default=True, default=False, help="Prompt to create new config")
@click.option("-o", "--check_optional", is_flag=True, show_default=True, default=True, help="Prompt for optional")
@click.option("-i", "--imported", type=str, help="Import config file and add modified")
@click.option("-j", "--json_config", type=str, help="provide json to place into infrastructure. Must be valid")
@click.pass_context
def create_config(ctx: click.Context, prompt: bool, check_optional: bool, imported: str, json_config: str):
    """
    Create a config.

    :param ctx: cli context
    :param prompt: ask for value if missing fields
    :param check_optional: ask for value of optional fields
    :param imported: import the json
    :param json_config: provide json in line

    """
    model = None
    if imported:
        if json_config:
            raise click.BadOptionUsage("json", f"If importing cannot set json")
        if not prompt:
            raise click.BadOptionUsage("prompt", f"If importing prompt must also be set")
        if prompt:
            import_config = json.load(open(imported, "r"))
            model = ConfigModel(
                os.path.join(
                    os.environ["TAHOE_ROOT_LOCATION"],
                    "tahoe_infrastructure", "custom_env"),
                import_config, prompt=True, prompt_optional=check_optional)
    else:
        model = ConfigModel(
            os.path.join(
                os.environ["TAHOE_ROOT_LOCATION"],
                "tahoe_infrastructure", "custom_env"),
            {} if not json_config else json.loads(json_config),
            prompt=prompt, prompt_optional=check_optional)
    if model is not None:
        if any({x.split(".")[0] == model.stack_prefix for x in os.listdir(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "tahoe_infrastructure", "custom_env"))}):
            click.confirm("A stack prefix of this name already exists at the file location confirm override", abort=True)
        else:
            config = read_configuration(ctx)
            config["context"][model.stack_prefix.get_value()] = "custom_env"
            write_configruation(ctx, config)
        model.write()


@cli.command()
@click.option("-n", "--name", type=str, required=True, help="Datasource name")
@click.option("-pl", "--pipeline", required=True, type=click.Choice(["incremental", "download", "query"]),
              default="download", show_default=True,
              help="Type of datasource pipeline to create")
@click.option("-v", "--validate",
              is_flag=True, help="Create validation file")
@click.option("-p", "--preprocess",
              is_flag=True,  help="Create preprocess file")
@click.pass_context
def create_datasource(ctx: click.Context, name: str, pipeline: str, validate: bool, preprocess: bool):
    """
    Create a datasource.
    
    :param ctx: cli context
    :param name: data prefix of datasource
    :param pipeline: type of step function to use
    :param validate: include validate file
    :param preprocess: include preprocess file
    """
    name = name.lower()
    validate_name(name)
    datasources_path = os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "datasources")
    # check if name is used
    if name in os.listdir(datasources_path):
        raise click.BadOptionUsage("name", f"Datasource name {name} already used. Select another name.")
    try:
        c_env = get_configs("custom_env")
        r_env = get_configs("required_env")
    except:
        ValueError("Failed to load existing configs run verify-existing-configs enable missing configuration")
    create_hierarchy(
        ctx.obj["datasource_infra_path"],
        ctx.obj["datasources_path"],
        name, ["download", "validation", "etl"])
    configs: ConfigModel
    for configs in c_env:
        datasource: Datasources = configs.datasources.get_value()
        datasource.add_datasource(name)
        configs.write()
    for configs in r_env:
        datasource: Datasources = configs.datasources.get_value()
        datasource.add_datasource(name)
        configs.write()

    # Use jinja template to create a datasource stack
    data = {"pipeline": pipeline, "name": capitalize(name), "validate": validate, "preprocess": validate}
    write_jinja_template(ctx, data, "nested_stack.py", os.path.join(
        ctx.obj["datasource_infra_path"], name, f"{name}_datasource_stack.py"))

    write_jinja_template(ctx, data, "empty.py", os.path.join(
        ctx.obj["datasource_infra_path"], name, f"__init__.py"))
    # Append to existing datasources
    update_datasources_base_stack(ctx, "datasource", name)


@cli.command()
@click.option("-n", "--name", type=str, required=True, help="Datasource name")
@click.option("-d", "--related_datasources", type=str, required=True,
              help="Comma deliminated datasources to be triggered by this")
@click.pass_context
def create_consumer(ctx: click.
                    Context, name: str, related_datasources: str):
    """
    Create a consumer.

    :param ctx: cli context
    :param name: data prefix of datasource
    :param related_datasources: datasources that the consumer uses data from
    """
    click.echo(print(os.listdir(os.path.join(os.path.dirname(__file__), 'templates')))
               )
    datasources = related_datasources.split(",")
    # Verify datasources
    valid_datasources = set(os.listdir(os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "datasources")))
    print(valid_datasources)
    for datasource in datasources:
        if datasource not in valid_datasources:
            raise click.UsageError("Datasources is not valid")

    name = name.lower()
    validate_name(name)
    consumers_path = os.path.join(os.environ["TAHOE_ROOT_LOCATION"], "consumers")
    # check if name is used
    if name in os.listdir(consumers_path):
        raise click.BadOptionUsage("name", f"Datasource name {name} already used. Select another name.")
    create_hierarchy(ctx.obj["consumer_infra_path"], consumers_path, name, ["etl"])
    # Append to existing consumer
    update_consumer_base_stack(ctx, "consumer", name, datasources)

    # Use jinja template to create a datasource stack
    data = {"name": capitalize(name)}
    write_jinja_template(ctx, data, "consumer_nested_stack.py", os.path.join(
        ctx.obj["consumer_infra_path"], name, f"{name}_consumer_stack.py"))

    write_jinja_template(ctx, data, "empty.py", os.path.join(
        ctx.obj["consumer_infra_path"], name, f"__init__.py"))


@cli.command()
@click.option("-n", "--name", type=str, required=True, help="Datasource name")
@click.option("-p", "--prompt",
              is_flag=True, show_default=True, default=False, help="Prompt to check before deleting each folder")
@click.pass_context
def remove_consumer(ctx: click.Context, name: str, prompt: str):
    """
    Remove a consumer.

    :param ctx: cli context
    :param name: data prefix of datasource
    :param prompt: ask before deleting files
    """
    check = click.confirm(f"Confirm delete of consumer {name}. FILES WILL BE UNRECOVERABLE. ")
    if check:
        undo_hierarchy(ctx.obj["consumer_infra_path"], ctx.obj["consumer_path"], name, prompt)
        delete_base_stack(ctx, "consumer", name)
    return


@cli.command()
@click.option("-l", "--location", type=str, required=True, help="Location to write file")
@click.option("-a", "--args",
              type=str, help="Comma deliminated args")
@click.option("-t", "--type",
              type=click.Choice(["PYSHELL", "PYSPARK"]),  help="PYSHELL or PYSPARK")
@click.pass_context
def create_etl_file(ctx: click.Context, location: str, args: str, type: str):
    """
    Creates basic etl file with parameters.

    :param location: Location to create file
    :param args: Args in job
    :param type: Type [PYSHELL, PYSPARK]
    """

    data = {
        "args": args.split(","),
        "type": type, "DEFAULT_IMPORTS": ctx.obj["default_imports"],
        "PYSPARK_DEFAULTS": ctx.obj["default_pyspark_imports"],
        "PYSHELL_DEFAULTS": ctx.obj["default_pyshell_imports"]}
    if os.path.exists(location):
        # Get current file
        sections = get_subsections({"IMPORTS", "SPARK", "CODE"}, location)

        for section in sections:
            if section == "IMPORTS":
                print(sections[section])
                sections[section] = list(filter(lambda x: in_imports(x, type, data), sections[section]))
                print(sections[section])
            code_section = "".join(sections[section]).rstrip("\n")
            if code_section != "":
                data[section.lower()] = code_section + "\n\n"
    data["DEFAULT_IMPORTS"] = "".join(data["DEFAULT_IMPORTS"])
    data["PYSPARK_DEFAULTS"] = "".join(data["PYSPARK_DEFAULTS"])
    data["PYSHELL_DEFAULTS"] = "".join(data["PYSHELL_DEFAULTS"])
    # Create using jinja template or update args on new values
    write_jinja_template(ctx, data, "etl.py", location)


if __name__ == '__main__':

    cli()
