from typing import List
from aws_cdk import NestedStack
from constructs import Construct
from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.consumers.docex_s3.stepfunctions.docex_s3_stepfuction import DocExS3StepFunction
from stacks.shared.constructs.tahoe_consumer_nested_stack import TahoeConsumerNestedStack
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack
import aws_cdk.aws_stepfunctions as sf

from aws_cdk import aws_databrew as databrew


class DocExS3Consumer(TahoeConsumerNestedStack):
    def __init__(self, scope: Construct, construct_id: str, *, data_prefix, build_context, base_context,
                 datasources: DependentDatasources) -> None:
        super().__init__(scope, construct_id, build_context, base_context, data_prefix)
        DocExS3StepFunction(self, data_prefix + "Sf", build_context, base_context, data_prefix,
                            self.sub_path, datasources).build_stepfunction()
