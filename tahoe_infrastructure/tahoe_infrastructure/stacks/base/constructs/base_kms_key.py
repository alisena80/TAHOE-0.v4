from ast import alias
from typing import List
from constructs import Construct
import aws_cdk.aws_kms as kms
import aws_cdk.aws_iam as iam
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig


class BaseKmsKey(TahoeConstruct):
    """
    A class to create the base encryption key for s3 and datasources.


    Attributes
    ----------
    kms_target : kms.Key
        Key used to encrypt data
    kms_policy: iam.Policy
        Iam policy added to any users/roles that need to decode/encode information

    Methods
    ----------
    def attach_policy(self, roles)

    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig):
        """
        Constructs kms key and policy

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
        """
        super().__init__(scope, id, build_context)
        self.default_kms = kms.Key.from_lookup(self, "defaultS3", alias_name="alias/aws/s3")
        self.kms_target = kms.Key(
            self, "kmsKey", alias=self.build_context.get_stack_prefix() + "/Tahoe")
        self.kms_policy = iam.Policy(self, "kmsKeyPolicy", policy_name="LLNL_User_Policies_r" +
                                     self.build_context.get_stack_prefix() + "_TahoeKmsKey")
        self.kms_policy.add_statements(
            iam.PolicyStatement(
                actions=["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:Describe*"],
                resources=[self.kms_target.key_arn]))

    def attach_policy(self, roles: List[iam.Role]):
        """
        attach roles to kms policy 

        Parameters
        ----------
            roles : List[iam.Role]
                List of roles to allow usage of key
        """
        self.kms_target.add_to_resource_policy(iam.PolicyStatement(actions=["kms:CreateGrant",
                                                                            "kms:ListGrants",
                                                                            "kms:RevokeGrant"],
                                                                   principals=roles,
                                                                   resources=[
                                                                       "*"],
                                                                   conditions={
            "Bool": {
                "kms:GrantIsForAWSResource": "true"
            }
        }))

        self.kms_target.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
                principals=roles, resources=["*"]))
