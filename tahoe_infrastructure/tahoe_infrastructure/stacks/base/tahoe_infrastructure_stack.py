from re import M
from unittest import result
import aws_cdk.aws_lakeformation as lakeformation
import aws_cdk.aws_resourcegroups as rg
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_athena as athena
import aws_cdk.aws_lambda as l
import os
import aws_cdk.aws_s3_deployment as s3deploy

from aws_cdk import Aspects, Stack, Tags
from constructs import Construct
from stacks.base.constructs.base_api import BaseApi

from stacks.base.constructs.base_classifiers import BaseClassifiers
from stacks.base.constructs.base_dynamo import BaseDynamo
from stacks.base.constructs.base_functions import BaseFunctions
from stacks.base.constructs.base_sns import BaseSns
from stacks.base.constructs.base_step_functions import BaseStepFunctions
from stacks.base.constructs.base_streams import BaseStreams
from stacks.shared.lib.build_config import BuildConfig
from stacks.base.constructs.base_kms_key import BaseKmsKey
from stacks.base.constructs.base_roles import BaseRoles
from stacks.base.constructs.datasources import Datasources
from stacks.base.constructs.base_buckets import BaseBuckets
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.aspects.verify_names import VerifyNames
from stacks.base.constructs.consumers import Consumers


class TahoeInfrastructureStack(Stack):
    """
    A class to represent a pipeline deployment


    Attributes
    ----------
    build_context  : BuildConfig
        Build configuration
    group  : rg.CfnGroup
        Group to track/tag resources
    kms  : BaseKmsKey
        Kms key configuration
    buckets: BaseBuckets
        Buckets configuration
    buckets: BaseBuckets
        Buckets configuration
    roles: BaseBuckets
        Roles configuration
    functions: BaseBuckets
        Functions configuration
    classifiers: BaseClassifiers
        Classifiers configuration
    base_context: BaseConfig
        base configuration to be exposed to datasources and consumers
    datasources: Datasources
        datasources configurations
    consumers: Consumers
        consumers configuration
    """

    def __init__(self, scope: Construct, construct_id: str, *, build_context: BuildConfig = "", **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.build_context = build_context
        self.logging_layer = l.LayerVersion(self, 'loggingsLayer',
                                    code=l.Code.from_asset(os.path.join("../base", "common", "lambda")),
                                    compatible_runtimes=[l.Runtime.PYTHON_3_9],
                                    description='A layer to add tahoe logging to lambda')
        self.build_context.set_logging_layer(self.logging_layer)
        # resource group
        self.group = rg.CfnGroup(
            self, 'myGroup', name=self.build_context.group_name("tahoe"))
        self.group.add_property_override('Tags', [
            {
                "Key": 'Project',
                "Value": 'Tahoe'
            },
            {
                "Key": 'Deployment',
                "Value": self.build_context.get_env()
            }

        ])
        
        # Resources
        self.vpc = ec2.Vpc.from_lookup(self, "vpc", tags={"TahoeVpc": "true"})

        self.kms = BaseKmsKey(self, "baseKms", build_context)

        self.buckets: BaseBuckets = BaseBuckets(
            self, "baseBuckets", self.build_context, kms_target=self.kms.kms_target, vpc=self.vpc)

        self.roles: BaseRoles = BaseRoles(
            self, "baseRoles", self.build_context, self.kms.kms_policy)


        self.sns: BaseSns = BaseSns(self, "baseSns", self.build_context)
        self.streams: BaseStreams = BaseStreams(self, "baseStream", build_context, self.roles.base_role)
        self.dynamo: BaseDynamo = BaseDynamo(self, "baseDynamo", build_context)
        self.functions: BaseFunctions = BaseFunctions(
            self, "baseFunctions", build_context, self.roles.base_role, email_sns_topic=self.sns.email_sns_topic,
            stream=self.streams, dynamo=self.dynamo)

        self.step_functions: BaseStepFunctions = BaseStepFunctions(self,
                                                                   'baseStepFunctions',
                                                                   build_context,
                                                                   query_sns_topic=self.sns.query_sns,
                                                                   base_role=self.roles.base_role,
                                                                   stream=self.streams)

        self.classfiers: BaseClassifiers = BaseClassifiers(
            self, "baseClassifiers", build_context)

        self.api: BaseApi = BaseApi(
            self, "baseApi", build_context, self.roles.base_role)

        s3deploy.BucketDeployment(self, "deploymentTahoeCommon",
                                  sources=[s3deploy.Source.asset(
                                      "../base/common")],
                                  destination_bucket=self.buckets.bootstrap_bucket,
                                  destination_key_prefix=self.build_context.get_s3_prefix() + "/shared",
                                  role=self.roles.base_role,
                                  memory_limit=256
                                  )
        if self.vpc:
            try:
                s3deploy.BucketDeployment(self, "deploymentWheelhouse",
                            sources=[s3deploy.Source.asset(
                                "../base/wheelhouse/wheelhouse")],
                            destination_bucket=self.buckets.wheelhouse_bucket,
                            role=self.roles.base_role,
                            memory_limit=256
                            )
            except RuntimeError as r:
                print("Exiting")
                

        self.api.add_env(
            "query_arn", self.step_functions.query_state_machine.state_machine_arn)

        self.step_functions.input_function_add_parameter("output_bucket", self.buckets.interim_bucket.bucket_name)

        self.base_context: BaseConfig = BaseConfig(
            self.buckets, self.roles, self.kms, self.functions, self.classfiers, self.step_functions, self.api,
            self.sns, self.streams, vpc=self.vpc)

        self.datasources: Datasources = Datasources(
            self, "baseData", self.build_context, self.base_context)

        self.consumers: Consumers = Consumers(
            self, "Consumer", self.build_context, self.base_context, self.datasources)

        kms_policies = [self.roles.base_role, self.roles.read_role]

        if self.build_context.get_optional("console_role_name"):
            # Predefined account roles/boundaries
            account_admin_role = iam.Role.from_role_arn(
                self, "principalArnUser",
                f"arn:{Stack.of(self).partition}:iam::{Stack.of(self).account}:role/{self.build_context.get_optional('console_role_name')}")
            kms_policies.append(account_admin_role)

        # add permissions to key
        self.kms.attach_policy(kms_policies)

        athena.CfnWorkGroup(self, "athenaWorkgroup", name=self.build_context.group_name(
            "query"), description=f"Workgroup for {self.build_context.get_stack_prefix()}",
                            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                                enforce_work_group_configuration=True, bytes_scanned_cutoff_per_query=1000000000,
                                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                                        encryption_option="SSE_KMS", kms_key=self.kms.default_kms.key_arn),
                                    output_location=self.buckets.raw_bucket.s3_url_for_object("query_results"))))

        # datalake location permissions
        rc = lakeformation.CfnResource(self, "rawDataLakeBucketLocation",
                                       resource_arn=self.buckets.raw_bucket.bucket_arn,
                                       use_service_linked_role=False, role_arn=self.roles.base_role.role_arn)

        ic = lakeformation.CfnResource(self, "interimDataLakeBucketLocation",
                                       resource_arn=self.buckets.interim_bucket.bucket_arn,
                                       use_service_linked_role=False, role_arn=self.roles.base_role.role_arn)

        cc = lakeformation.CfnResource(self, "curatedDataLakeBucketLocation",
                                       resource_arn=self.buckets.curated_bucket.bucket_arn,
                                       use_service_linked_role=False, role_arn=self.roles.base_role.role_arn)

        pc = lakeformation.CfnResource(self, "preprocDataLakeBucketLocation",
                                       resource_arn=self.buckets.preprocess_bucket.bucket_arn,
                                       use_service_linked_role=False, role_arn=self.roles.base_role.role_arn)

        principal = lakeformation.CfnPermissions.DataLakePrincipalProperty(
            data_lake_principal_identifier=self.base_context.get_pipeline_role().role_arn)

        lakeformation.CfnPermissions(
            self, "rawLocationPermissions", data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=self.buckets.raw_bucket.bucket_arn)),
            permissions=["DATA_LOCATION_ACCESS"]).add_depends_on(rc)
        lakeformation.CfnPermissions(
            self, "curatedLocationPermissions", data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=self.buckets.curated_bucket.bucket_arn)),
            permissions=["DATA_LOCATION_ACCESS"]).add_depends_on(cc)

        lakeformation.CfnPermissions(
            self, "interimLocationPermissions", data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=self.buckets.interim_bucket.bucket_arn)),
            permissions=["DATA_LOCATION_ACCESS"]).add_depends_on(ic)
        lakeformation.CfnPermissions(
            self, "prepreocessLocationPermissions", data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=self.buckets.preprocess_bucket.bucket_arn)),
            permissions=["DATA_LOCATION_ACCESS"]).add_depends_on(pc)
        for key, tag in self.build_context.get_tags().items():
            Tags.of(self).add(key, tag)
        # validate names and remove cdk auto generated items
        Aspects.of(self).add(VerifyNames(build_context))
