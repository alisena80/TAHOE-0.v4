
from stacks.datasources.shared.query_retrieval_step_function import QueryRetrievalStepFunction
from stacks.shared.constructs.etl_job import Etl
from ...shared.constructs.pyshell_job import Pyshell
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack

import aws_cdk.aws_stepfunctions as sf


class ShodanDatasourceInfrastructureStack(TahoeDatasourceNestedStack):
    """
    Creates a shodan data source stack.

    Requires shodan_base_url and shodan_secret_key in cdk.json.
    """
    def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix,
                 build_context, base_context, log) -> None:
        super().__init__(scope, construct_id, build_context, data_prefix, base_context, log)

        self.logger.info("Compiling shodan datasource")
        tahoe_commons = self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_tahoe_common())
        extra_python_files = [tahoe_commons]
        shodan_secret_key = self.build_context.get_credential("shodan_secret_key")
        shodan_base_url = self.build_context.get_credential("shodan_base_url")
        custom_query_function = Pyshell(self, "queryEtl", build_context, data_prefix, base_context, "load", self.sub_path,
                                    sub_folder="download",
                                    python_files=extra_python_files,
                                    QUERY=sf.JsonPath.json_to_string(sf.JsonPath.object_at("$.data.query")),
                                    DETAILS=sf.JsonPath.json_to_string(
                                          sf.JsonPath.object_at("$.data.additional_details")),
                                    EXECUTION=sf.JsonPath.string_at("$$.Execution.Id"),
                                    SHODAN_SECRET_KEY=shodan_secret_key,
                                    SHODAN_BASE_URL=shodan_base_url)
        QueryRetrievalStepFunction(self, "retrieval", build_context, data_prefix, base_context,
                                   self.etl_databases_config, self.sub_path,
                                   log=self.log, custom_query_function=custom_query_function).build_stepfunction()
