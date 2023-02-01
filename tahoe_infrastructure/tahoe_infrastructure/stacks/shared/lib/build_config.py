from aws_cdk.aws_s3 import Bucket
from aws_cdk import Fn
import os
import boto3
from stacks.shared.lib.enumerations import GlueVersions
from stacks.shared.lib.regional_config import SandboxConfig
from collections import defaultdict


class BuildConfig():
    def __init__(
            self, stack_prefix: str, s3_prefix: str, region: str, env: str, account: str, sandbox: bool,
            external_resources: dict, tags: dict = {},
            datasource_config: dict = None, optional: dict = None, credentials: dict = None, redshift_schemas: dict = None,
            cdk_log_level: str = "INFO"):
        self._REGION = region
        region_split = region.split("-")
        self._STACK_PREFIX = stack_prefix + "".join([info[0] for info in region_split])
        self._S3_PREFIX = s3_prefix
        self._ENV = env
        self._ACCOUNT = account
        self._SANDBOX = sandbox
        self._EXTERNAL_RESOURCES = external_resources
        self._DATASOURCE_CONFIG = datasource_config
        self._TAGS: dict = tags
        self._OPTIONAL = defaultdict(lambda: None, optional)
        self._CREDENTIALS = defaultdict(lambda: None, credentials)
        self._REDSHIFT_SCHEMAS = redshift_schemas
        self._CDK_LOG_LEVEl = cdk_log_level
        self._LOGGING_LAYER = None
        if "gov" in self._REGION:
            self._PARTITION = "aws-us-gov"
        else:
            self._PARTITION = "aws"
        os.chdir("../base")

        with open(os.getcwd() + "/wheelhouse/3.6requirements.txt") as modules:
            self._ETL_PYTHON_MODULES_P36 = {x.strip("\n") for x in modules}

        with open(os.getcwd() + "/wheelhouse/3.7requirements.txt") as modules:
            self._ETL_PYTHON_MODULES_P37 = {x.strip("\n") for x in modules}

    def get_cdk_log_level(self):
        return self._CDK_LOG_LEVEl

    def get_stack_prefix(self):
        return self._STACK_PREFIX

    def get_optional(self, key):
        return self._OPTIONAL[key]

    def set_credential(self, key, value):
        self._CREDENTIALS[key] = value
        
    def get_credential(self, key):
        cred = None
        if key in self._CREDENTIALS:
            cred = self._CREDENTIALS[key]
        else:
            raise ValueError(f"Missing credential {key}")
        return cred

    def get_account(self):
        return self._ACCOUNT

    def get_s3_prefix(self):
        return self._S3_PREFIX

    def get_region(self):
        return self._REGION

    def get_env(self):
        return self._ENV

    def get_partition(self):
        return self._PARTITION

    def set_logging_layer(self, logging_layer):
        self._LOGGING_LAYER = logging_layer

    def get_logging_layer(self):
        return self._LOGGING_LAYER

    def get_datasource_config(self, datasource):
        if datasource in self._DATASOURCE_CONFIG:
            return self._DATASOURCE_CONFIG[datasource]
        else:
            raise ValueError(f"Missing {datasource} config")

    def get_pydeequ_jar(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["pydeequ_jar"]

    def get_hudi_jar(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["hudi_jar"]

    def get_calcite_jar(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["calcite_jar"]

    def get_libfb_jar(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["libfb_jar"]

    def get_pydeequ_zip(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["pydeequ_zip"]

    def get_tahoe_logger(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["tahoe_logger"]

    def get_tahoe_common(self):
        return self.get_s3_prefix() + self._EXTERNAL_RESOURCES["tahoe_common"]

    def get_tags(self) -> dict:
        return self._TAGS

    def is_sandbox(self):
        return self._SANDBOX

    def set_sandbox_config(self, sandbox: SandboxConfig):
        self.sandbox_config = sandbox

    def get_sandbox_config(self):
        return self.sandbox_config

    def get_etl_python_modules(self, version: GlueVersions = None):
        """
        Get valid python modules depending on installed
        """
        if version == GlueVersions.PYSHELL:
            return self._ETL_PYTHON_MODULES_P36
        elif version == GlueVersions.PYSPARK:
            return self._ETL_PYTHON_MODULES_P37
        elif version == "any":
            return self._ETL_PYTHON_MODULES_P36 + self._ETL_PYTHON_MODULES_P37
        else:
            raise ValueError(f"Python version {version} is not valid")

    def get_asset_bucket(self, truncated=False):
        if truncated:
            return "cdk-hnb659fds-assets-{}-{}".format(self._ACCOUNT, self._REGION)
        return "cdk-hnb659fds-assets-{}-{}/{}".format(self._ACCOUNT, self._REGION, self._S3_PREFIX)

    def resource_arn(self, type, pre="", ignore_prefix=False):
        if ignore_prefix:
            return "arn:{}:{}:{}:{}:{}".format(self._PARTITION, type, self._REGION, self._ACCOUNT, pre)
        return "arn:{}:{}:{}:{}:{}{}*".format(self._PARTITION, type, self._REGION, self._ACCOUNT, pre,
                                              self._STACK_PREFIX)

    def regionless_resource_arn(self, type, name, not_all=False):
        if not_all:
            return "arn:{}:{}:::{}".format(self._PARTITION, type, name)
        return "arn:{}:{}:::{}*".format(self._PARTITION, type, name)

    def policy_name(self, name):
        return "LLNL_User_Policies_{}_{}".format(self._STACK_PREFIX, name)

    def group_name(self, name):
        return "{}_group_{}".format(self._STACK_PREFIX, name)

    def role_name(self, name):
        return "LLNL_User_Roles_{}_{}".format(self._STACK_PREFIX, name)

    def user_name(self, name):
        return "LLNL_User_{}_{}".format(self._STACK_PREFIX, name)

    def s3_name(self, name):
        return "{}-s3-{}".format(self._STACK_PREFIX, name)

    def api_name(self):
        return "{}-api".format(self._STACK_PREFIX)

    def lambda_name(self, name):
        return "{}_lambda_{}".format(self._STACK_PREFIX, name)

    def sns_name(self, name):
        return "{}_sns_{}_topic".format(self._STACK_PREFIX, name)

    def glue_name(self, prefix, name):
        return "{}_glue_{}_{}".format(self._STACK_PREFIX, prefix, name)

    def glue_name_function(self, prefix, name, id):
        return "{}_glue_{}_{}_{}".format(self._STACK_PREFIX, prefix, name, id)

    def sf_name(self, prefix, name):
        return "{}_sf_{}_{}".format(self._STACK_PREFIX, prefix, name)

    def redshift_name(self, name):
        return "{}_redshift_{}".format(self._STACK_PREFIX, name)

    def s3_url(self, bucket: Bucket, name: str):
        return bucket.s3_url_for_object("/".join(name.split("_")))

    def redshift_schema(self, name: str):
        if name not in self._REDSHIFT_SCHEMAS:
            return ""
        return self._REDSHIFT_SCHEMAS[name]
