from aws_cdk import NestedStack

from aws_cdk import Aspects, Stack
from constructs import Construct

import aws_cdk.aws_lakeformation as lakeformation
import aws_cdk.aws_iam as iam
import aws_cdk.aws_redshift_alpha as redshift
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_docdb as docdb
import aws_cdk.aws_kms as kms
from aws_cdk import RemovalPolicy
import aws_cdk.aws_ec2 as ec2
from aws_cdk import RemovalPolicy
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue as glue
from aws_cdk import CfnOutput
from stacks.shared.lib.build_config import BuildConfig


class TahoeSandboxStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, build_context: BuildConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.build_context = build_context

        # KMS key that all tahoe related roles need to decrypt
        self.kms_target = kms.Key(self, "kmsKey", alias="TahoeData")

        self.vpc = ec2.Vpc.from_lookup(self, "vpc", tags={"TahoeVpc": "true"})

        # If no public subnets are available, try the private ones
        try:
            subnet_id = self.vpc.public_subnets[0].subnet_id
            availability_zone = self.vpc.public_subnets[0].availability_zone
        except IndexError:
            subnet_id = self.vpc.private_subnets[0].subnet_id
            availability_zone = self.vpc.private_subnets[0].availability_zone

        self.redshift = redshift.Cluster(self, "redshift", master_user=redshift.Login(master_username="awsuser"),
                                         vpc=self.vpc,
                                         encryption_key=self.kms_target,
                                         cluster_type=redshift.ClusterType.SINGLE_NODE,
                                         default_database_name="dev",
                                         vpc_subnets=ec2.SubnetSelection(
                                             availability_zones=[self.build_context.get_region() + "a",
                                                                 self.build_context.get_region() + "b"]),
                                         removal_policy=RemovalPolicy.DESTROY

                                         )

        self.redshift_connection = glue.CfnConnection(
            self, "dbConnectionRedshift", catalog_id=self.build_context.get_account(),
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="redshift_connection", connection_type="JDBC",
                connection_properties={"JDBC_CONNECTION_URL": "jdbc://" + self.redshift.cluster_endpoint.socket_address +
                                       "/dev", "JDBC_ENFORCE_SSL": "false",
                                       "USERNAME": self.redshift.secret.secret_value_from_json("username").to_string(),
                                       "PASSWORD": self.redshift.secret.secret_value_from_json("password").to_string()},
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=subnet_id, availability_zone=availability_zone,
                    security_group_id_list=[x.security_group_id for x in self.redshift.connections.security_groups])))
