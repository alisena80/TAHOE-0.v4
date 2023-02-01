from typing import List
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_sns as sns

from constructs import Construct
from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.consumers.shared.query_consumer_step_function import QueryConsumerStepFunction
from stacks.shared.constructs.etl_job import Etl


class CdmTestStepFunction(QueryConsumerStepFunction):

    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path,
                 datasources: DependentDatasources):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path, datasources)

        cdm_dedup = Etl(self, "mitreDedupe", build_context, data_prefix, base_context, "combine_cyhy", self.sub_path,
                        **(self.query_input),
                        CYHY_INPUT=self.datasources.get_datasource("cyhy").etl_databases_config.interim_database.database_input.name,
                        STACK_PREFIX=self.build_context.get_stack_prefix(),
                        S3_OUTPUT=self.base_context.get_curated_bucket().bucket_name)

        self.set_custom_definition(cdm_dedup.definition)
