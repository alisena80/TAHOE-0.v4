
from stacks.datasources.shared.query_retrieval_step_function import QueryRetrievalStepFunction
from stacks.shared.constructs.etl_job import Etl
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack

import aws_cdk.aws_stepfunctions as sf


class CdmDatasourceInfrastructureStack(TahoeDatasourceNestedStack):
   def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix, build_context, base_context, log) -> None:
        super().__init__(scope, construct_id, build_context, data_prefix, base_context, log)
        self.logger.info("Compiling cdm datasource")
        custom_query_function = Etl(self, "queryEtl", build_context, data_prefix, base_context, "load", self.sub_path,
                                    sub_folder="download",
                                    python_modules=["awswrangler"],
                                    QUERY=sf.JsonPath.json_to_string(sf.JsonPath.object_at("$.data.query")),
                                    DETAILS=sf.JsonPath.json_to_string(sf.JsonPath.object_at("$.data.additional_details")),
                                    EXECUTION=sf.JsonPath.string_at("$$.Execution.Id"))

        QueryRetrievalStepFunction(self, "retrieval", build_context, data_prefix, base_context, self.etl_databases_config, self.sub_path,
                                  log=self.log, custom_query_function=custom_query_function).build_stepfunction()
