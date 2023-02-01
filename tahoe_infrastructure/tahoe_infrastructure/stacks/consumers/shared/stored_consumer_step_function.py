from constructs import Construct
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as sf


from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct


class StoredConsumerStepFunction(TahoeStepFunctionConstruct):
    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path,
                 datasources: DependentDatasources):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        self.datasources = datasources

    def build_stepfunction(self):
        super().build_stepfunction()
        self.function_trigger([x.sns_config.ingested_done_topic for x in self.datasources.stored.values()])
