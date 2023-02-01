from constructs import Construct
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_glue as glue
from aws_cdk import Duration
from stacks.shared.constructs.etl_database_config import EtlDatabaseConfig

from stacks.shared.constructs.etl_job import Etl
from stacks.shared.constructs.crawler import Crawler
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack
from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct


class QueryRetrievalStepFunction(TahoeStepFunctionConstruct):
    '''
    Query Step retrival to get data which is too large to fully download. This step function is triggered by API and retrives data based on an api query. 
    The infomration is then stored in S3 and a reference is sent to the sns which triggers the consumer steps.
    '''

    def __init__(self, scope: TahoeDatasourceNestedStack, id: str, build_context, data_prefix, base_context,
                 databases: EtlDatabaseConfig, sub_path, *, log=True, custom_query_function=None, custom_crawler=None):
        """
        Constructs query pipeline step function

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            sub_path : str
                base path of file [datasources,base,consumer]
            log: bool
                track status of the pipleine
            custom_query_function: Etl or Pyshell
                custom function required since configuration changes required from te default ETL created.
                Etl(self, "queryEtl", build_context, data_prefix, base_context, "load", self.sub_path, S3OUTPUT=self.build_context.s3_url(self.base_context.get_interim_bucket(), self.data_prefix))

        """
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        if custom_query_function:
            self.query_job = custom_query_function
        else:
            self.query_job = Etl(
                self, "queryEtl", build_context, data_prefix, base_context, "load", self.sub_path,
                sub_folder="download", QUERY=sf.JsonPath.json_to_string(sf.JsonPath.object_at("$.data.query")),
                DETAILS=sf.JsonPath.json_to_string(sf.JsonPath.object_at("$.data.additional_details")),
                EXECUTION=sf.JsonPath.string_at("$$.Execution.Id"))

        if custom_crawler:
            self.crawler = custom_crawler
        else:
            self.crawler = Crawler(
                self, "rawCrawler", build_context, data_prefix, base_context, databases.interim_database.database_input.
                name, self.base_context.get_interim_bucket(),
                target=glue.CfnCrawler.TargetsProperty(
                    s3_targets=[glue.CfnCrawler.S3TargetProperty(
                        path=self.build_context.s3_url(
                            self.base_context.get_interim_bucket(),
                            self.data_prefix + "/persisted"))]))

        success = sf.Pass(self, "passToQuery", parameters=self.base_context.get_step_functions().query_sf_parameters())
        persist_or_complete = sf.Choice(self, "perisistOrQuery")

        self.definition = self.query_job.definition.add_retry(
            interval=Duration.minutes(5),
            errors=["Glue.ConcurrentRunsExceededException"]).add_catch(
            handler=self.status_function, result_path="$.errors")
        self.crawler.set_status_function(self.status_function)
        self.crawler.set_log(log)
        persist_or_complete.when(
            sf.Condition.boolean_equals("$.data.additional_details.persist", True),
            self.crawler.completed(success)).when(
            sf.Condition.boolean_equals("$.data.additional_details.persist", False),
            success)
        scope.queryable()
        self.set_custom_definition(self.query_job.definition.next(persist_or_complete))

    def build_stepfunction(self):
        super().build_stepfunction()
        self.base_context.get_step_functions().input_function_add_parameter(
            self.data_prefix + "_arn", self.state_machine.state_machine_arn)
