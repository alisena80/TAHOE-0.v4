from typing import List
from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.consumers.cdm_test_consumer.stepfunctions.cdm_test_stepfunction import CdmTestStepFunction
from stacks.shared.constructs.tahoe_consumer_nested_stack import TahoeConsumerNestedStack
from constructs import Construct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig


class CdmTestConsumer(TahoeConsumerNestedStack):
    def __init__(self, scope: Construct, construct_id: str, *, data_prefix: str, build_context: BuildConfig,
                 base_context: BaseConfig, datasources: DependentDatasources) -> None:
        super().__init__(scope, construct_id, build_context, data_prefix, base_context)
        CdmTestStepFunction(self, "cdmStep", build_context, base_context, data_prefix, self.sub_path, datasources).build_stepfunction()
