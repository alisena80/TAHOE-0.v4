from aws_cdk import DefaultStackSynthesizer, Stack
from constructs import Construct

import aws_cdk.aws_lakeformation as lakeformation
import aws_cdk.aws_iam as iam
from stacks.shared.lib.build_config import BuildConfig


class TahoeSingletonStack(Stack):
    def __init__(self,scope: Construct, construct_id: str, build_context: BuildConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        admins = []
        synth : DefaultStackSynthesizer = kwargs["synthesizer"]
        if build_context.get_optional("console_role_name"):
            self.account_admin_role = iam.Role.from_role_arn(
                self, "principalArnUser", f"arn:{Stack.of(self).partition}:iam::{Stack.of(self).account}:role/{build_context.get_optional('console_role_name')}")
            datalake_dpp1 = lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.account_admin_role.role_arn)
            admins.append(datalake_dpp1)
        self.cdk_admin_role = iam.Role.from_role_arn(
            self, "cdkArnUser", synth.cloud_formation_execution_role_arn.replace('${AWS::Partition}', build_context.get_partition()))
        datalake_dpp2 = lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
            data_lake_principal_identifier=self.cdk_admin_role.role_arn)
        admins.append(datalake_dpp2)
 
        
    
        lakeformation.CfnDataLakeSettings(self, "cdkCfnExecRoleAdminPermission", admins=admins)
       