from aws_cdk.aws_ec2 import Vpc
from aws_cdk.aws_redshift_alpha import Cluster
from stacks.base.constructs.base_api import BaseApi
from stacks.base.constructs.base_classifiers import BaseClassifiers
from stacks.base.constructs.base_functions import BaseFunctions
from stacks.base.constructs.base_roles import BaseRoles
from stacks.base.constructs.base_kms_key import BaseKmsKey
from stacks.base.constructs.base_buckets import BaseBuckets
from stacks.base.constructs.base_sns import BaseSns
from stacks.base.constructs.base_streams import BaseStreams
from stacks.base.constructs.base_step_functions import BaseStepFunctions


class BaseConfig():
    def __init__(
            self, buckets, roles, kms, functions, classifiers, step_functions, api_config, sns, streams, *, vpc=None,
            redshift=None, email_sns_topic=None):
        self.roles: BaseRoles = roles
        self.kms: BaseKmsKey = kms
        self.buckets: BaseBuckets = buckets
        self.functions: BaseFunctions = functions
        self.classifiers: BaseClassifiers = classifiers
        self.api: BaseApi = api_config
        self.step_functions: BaseStepFunctions = step_functions
        self.sns: BaseSns = sns
        self.vpc: Vpc = vpc
        self.streams: BaseStreams = streams
        self.redshift_cluster: Cluster = redshift

    def get_api(self):
        return self.api

    def get_raw_bucket(self):
        return self.buckets.raw_bucket

    def get_interim_bucket(self):
        return self.buckets.interim_bucket

    def get_curated_bucket(self):
        return self.buckets.curated_bucket

    def get_preproc_bucket(self):
        return self.buckets.preprocess_bucket

    def get_wheelhouse_bucket(self):
        return self.buckets.wheelhouse_bucket

    def get_bootstrap_bucket(self):
        return self.buckets.bootstrap_bucket

    def get_pipeline_role(self):
        return self.roles.base_role

    def get_read_only_user_group(self):
        return self.roles.read_only_user_group

    def get_read_only_role(self):
        return self.roles.read_role

    def get_crawler_run_function(self):
        return self.functions.crawler_run_function

    def get_crawler_status_function(self):
        return self.functions.crawler_status_function

    def get_pipeline_status_function_task(self):
        return self.functions.pipeline_status_function

    def get_functions(self):
        return self.functions

    def get_json_array_classifier(self):
        return self.classifiers.json_array.json_classifier.name

    def get_key(self):
        return self.kms.kms_target

    def get_vpc(self):
        return self.vpc

    def get_redshift_cluster(self):
        return self.redshift_cluster

    def get_email_sns_topic(self):
        return self.sns.email_sns_topic

    def get_query_sns_topic(self):
        return self.sns.query_sns

    def get_streams(self):
        return self.streams

    def get_step_functions(self):
        return self.step_functions
