from constructs import Construct
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as sf


from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct
from stacks.shared.lib.dependent_datasources import DependentDatasources


class QueryConsumerStepFunction(TahoeStepFunctionConstruct):
    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path,
                 datasources: DependentDatasources):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        self.datasources = datasources
        self.query_input = {}
        for x in self.datasources.queried.values():
            self.query_input[x.data_prefix.upper() + "_INPUT"] = sf.JsonPath.string_at("$.input." + x.data_prefix)

    def build_stepfunction(self):
        super().build_stepfunction()
        # To add subscriptions to base stack created sns need to import arn into the nested stack
        self.function_trigger([sns.Topic.from_topic_arn(
            self, "queryTopic", self.base_context.get_query_sns_topic().topic_arn)],
            filter={"Consumer": sns.SubscriptionFilter.string_filter(allowlist=[self.data_prefix])})
