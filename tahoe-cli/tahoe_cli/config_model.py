import json
import os
import click
import enum
from tahoe_cli.utils import get_configs_raw, find_tahoe_root
from typing import Union

find_tahoe_root()

loaded_datasources = get_configs_raw("required_env")[0]["datasources"]

class ConfigType():
    def __init__(self, prompt):
        self.prompt = prompt
    def to_json(self):
        resp = {}
        ignore = {"prompt"}
        for key, val in self.__dict__.items():
            if key in ignore:
                continue
            if type(val) is Value:
                if val.get_value() == "None" and val.is_optional:
                    continue
                if val.expected_type in ConfigType.__subclasses__():
                    resp[key] = val.get_value().to_json()
                else:
                    resp[key] = val.get_value()
        return resp

    def was_prompted(self):
        prompted = False
        for key, val in self.__dict__.items():
            if type(val) is Value:
                if val.was_prompted:
                    return True
                if val.expected_type in ConfigType.__subclasses__():
                    if val.get_value().was_prompted():
                        return True
        return prompted

    def set_prompt(self, prompt):
        self.prompt = prompt

class Value():
    def __init__(self, model, key, expected_type: object, prompt, is_optional=False, desc=""):
        """
        :param model: The model
        :param key: The key to verify in the model
        :param expected_type: Type needed to verify input
        :param prompt: Wether to prompt the user for the missing input
        """
        self.key = key
        self.value: Union[ConfigType, enum.Enum, str] = None
        self.expected_type = expected_type
        self.desc = desc
        self.is_optional = is_optional
        self.was_prompted = False
        if self.key in model:
            self.value = model[key]
        elif self.key not in model and prompt and self.expected_type not in ConfigType.__subclasses__():
            self.value = self.request_prompt()
        if self.expected_type in ConfigType.__subclasses__():
            if self.value is None:
                if prompt:
                    self.show_desc()
                self.value = {}
        if self.value is not None and not isinstance(self.value, expected_type):
            # attempt to convert
            while True:
                try:
                    if self.expected_type in ConfigType.__subclasses__():
                        self.value = expected_type(self.value, prompt)
                    elif expected_type is dict:
                        self.value = json.loads(self.value)
                    else:
                        self.value = expected_type(self.value)
                    break
                except Exception as e:
                    if is_optional and self.value == "None":
                        break
                    if '_member_names_' in self.expected_type.__dict__:
                        click.echo(
                            f"Error with input expects {str(self.expected_type)} ex. {self.expected_type.__dict__['_member_names_']}")
                    else:
                        click.echo(f"Error with input expects {str(self.expected_type)}")
                    if prompt:
                        # convert fails raise error
                        self.value = self.request_prompt()
                    else:
                        raise e
        if self.value is None and not is_optional:
            raise ValueError(f"{self.key} is required")

    def show_desc(self):
        if '_member_names_' in self.expected_type.__dict__:
            click.echo(f"\n{self.expected_type.__dict__['_member_names_']} {self.desc}")
        else:
            click.echo(f"\n{self.desc}")
    def request_prompt(self):
        self.was_prompted = True
        self.show_desc()
        value = click.prompt(f"Enter {self.key}")
        return value

    def set_expected_type(self, tp):
        self.expected_type = tp

    def get_value(self):
        if type(self.value) in enum.Enum.__subclasses__():
            return self.value.value
        return self.value


class TypesEnumMeta(enum.EnumMeta):
    def __call__(cls, value, *args, **kw):
        if isinstance(value, str):
            value = {'TRUE': True, 'FALSE': False}.get(value)
        return super().__call__(value, *args, **kw)


class Bool(enum.Enum, metaclass=TypesEnumMeta):
    TRUE = True
    FALSE = False


class RedshiftSchemas(ConfigType):
    def __init__(self, schemas, prompt=False):
        super().__init__(prompt)
        self.nvd = Value(schemas, "nvd", expected_type=str, prompt=prompt, desc="nvd schema name")
        self.cyhy = Value(schemas, "cyhy", expected_type=str, prompt=prompt, desc="cyhy schema name")


class Optional(ConfigType):
    def __init__(self, schemas, prompt=False):
        super().__init__(prompt)
        self.auto_delete_buckets = Value(schemas, "auto_delete_buckets",
                                         expected_type=Bool, prompt=prompt, is_optional=True, desc="Delete buckets storing data on deletion. None to skip.")
        self.sync_cdk_to_etl_parameters = Value(
            schemas, "sync_cdk_to_etl_parameters", expected_type=Bool, prompt=prompt, is_optional=True, desc="Sync ETL parameters from cdk resources to etl scripts. None to skip")
        self.console_role_name = Value(schemas, "console_role_name", expected_type=str, prompt=prompt, is_optional=True, desc="Console role name to give admin access to must exist in console prior to deploy. None to skip")


class Credentials(ConfigType):
    def __init__(self, creds, prompt=False):
        super().__init__(prompt)
        self.shodan_secret_key = Value(creds, "shodan_secret_key", expected_type=str, prompt=prompt, desc="AWS Secret name to access shodan")
        self.shodan_base_url = Value(creds, "shodan_base_url", expected_type=str, prompt=prompt, desc="base url to acess shodan")

class DatasourceOptions(ConfigType):
    def __init__(self, deployable, prompt=False):
        super().__init__(prompt)
        self.deploy = Value(deployable, "deploy", expected_type=Bool, prompt=prompt, desc="deploy the resource")
        self.log = Value(deployable, "log", expected_type=Bool, prompt=prompt)

        

class Datasources(ConfigType):
    def __init__(self, datasources, prompt=False):
        super().__init__(prompt)
        for val in loaded_datasources:
            setattr(self, val, Value(datasources, val, expected_type=DatasourceOptions, prompt=prompt, desc=f"Datasource {val}"))

    def add_datasource(self, datasource):
        setattr(self, datasource, Value({datasource: {"deploy": True, "log": False}}, datasource, expected_type=DatasourceOptions, prompt=self.prompt))

    def remove_datasource(self, datasource):
        try:
            delattr(self, datasource)
        except AttributeError:
            return


class LogLevel(enum.Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    WARN = "WARN"


class ConfigModel(ConfigType):
    def __init__(self, location, model, prompt=False, prompt_optional=False):
        super().__init__(prompt)
        try:
            self.location = location
            self.stack_prefix = Value(model, "stack_prefix", expected_type=str, prompt=prompt, desc="Prefix for AWS resources")
            self.s3_prefix = Value(model, "s3_prefix", expected_type=str, prompt=prompt, desc="Prefix for s3 folder deployed resources")
            self.cdk_log_level = Value(model, "cdk_log_level", expected_type=LogLevel, prompt=prompt, desc="Log level for deployment")
            self.sandbox = Value(model, "sandbox", expected_type=Bool, prompt=prompt, desc="Generate sandbox stack regionwide resources")
            self.optional = Value(model, "optional", expected_type=Optional, prompt=prompt_optional, desc="Optional configs")
            self.redshift_schemas = Value(model, "redshift_schemas", expected_type=RedshiftSchemas, prompt=prompt, desc="Redshift schemas")
            self.credentials = Value(model, "credentials", expected_type=Credentials, prompt=prompt, desc="Credential details")
            self.tags = Value(model, "tags", expected_type=dict, prompt=prompt, is_optional=True, desc="Json of tags to aws resources")
            self.datasources = Value(model, "datasources", expected_type=Datasources, prompt=prompt, desc="Deployable datasources datasources present in synth_init used as source of truth")
        except Exception as e:
            raise ValueError(location,self.stack_prefix.get_value(), str(e))

    def write(self):
        json.dump(self.to_json(), open(os.path.join(self.location, f"{self.stack_prefix.get_value()}.json"), "w"))
    def __eq__(self, obj):
        return True if isinstance(obj, ConfigModel) and obj.stack_prefix.get_value() == self.stack_prefix.get_value() else False