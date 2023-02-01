# Retrival is the same where stored in collection bucket
# validate incrementally pulled data to current glue table
# preprocess incrementally pulls from the raw bucket
# Creates hudi table
# no crawler run
# hudi df -> dynamic frame -> relationalize -> output
# crawler run for relationalized
# curated uses relationalized

# enable bookmark in etl


from constructs import Construct
from stacks.shared.constructs.crawler import Crawler
from stacks.shared.constructs.etl_database_config import EtlDatabaseConfig
from stacks.shared.constructs.etl_job import Etl
from stacks.shared.constructs.sns_config import SnsConfig
from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sft


class HudiIncremetalStepFunction(TahoeStepFunctionConstruct):

    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context, sns_config: SnsConfig,
                 databases: EtlDatabaseConfig, sub_path, *, validate=False,
                 custom_interim_crawler: Crawler = None, custom_etl: Etl = None, log=True, custom_validate: Etl = None):

        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        if validate or custom_validate:
            if custom_validate:
                self.validate = custom_validate
            else:
                self.validate = Etl(
                    self, "validationFn", build_context, data_prefix, base_context, "validate", self.sub_path,
                    python_modules=["deepdiff"],
                    concurrency=25, python_files=[self.tahoe_commons],
                    sub_folder="validation", DATABASE=databases.raw_database.database_input.name,
                    UPDATE_LOCATION=sf.JsonPath.string_at("$.update_location"),
                    TABLE_NAME=sf.JsonPath.string_at("$.table_name"),)
            if log:
                # Create log states and wait log for failed schema creates
                log_validate = self.base_context.functions.pipeline_database_task(
                    self, "validated", "VALIDATED", self.data_prefix)
                self.validate.definition = self.validate.definition.next(
                    log_validate)

             # Wait for human approver if validation fails
            log_approval = self.base_context.functions.pipeline_database_wait_task(
                self, "approval", "FROZEN", self.data_prefix).add_catch(
                errors=["States.TaskFailed,States.Timeout"],
                handler=self.status_function, result_path="$.errors")

            # Use log states if required
            self.validate.definition = self.validate.definition.add_catch(handler=log_approval,
                                                                          result_path="$.input")

            self.definition = self.definition.next(self.validate.definition)

        # preprocess for datasource
        self.preprocess = Etl(
            self, "preprocessFn", build_context, data_prefix, base_context, "preprocess_to_hudi", self.sub_path,
            jars=self.hudi_jars, python_files=[self.tahoe_commons],
            concurrency=25, DATA_PREFIX=self.data_prefix, UPDATE_LOCATION=sf.JsonPath.string_at("$.update_location"),
            TABLE_NAME=sf.JsonPath.string_at("$.table_name"),
            PREPROCESS_BUCKET=self.base_context.get_preproc_bucket().bucket_name,
            RAW_DATABASE=databases.raw_database.database_input.name)
        self.definition = self.definition.next(self.preprocess.definition.add_catch(
            handler=self.status_function, result_path="$.errors"))

        # Relationalize hudi tables
        self.interim = Etl(
            self, "interimFn", build_context, data_prefix, base_context, "relationalize", self.sub_path,
            jars=[self.pydeequ_jar] + self.hudi_jars, concurrency=25, python_files=[self.tahoe_commons,
                                                                                    self.pydeequ_zip],
            STACK_PREFIX=self.build_context.get_stack_prefix(),
            DATA_PREFIX=self.data_prefix, PREPROCESS_BUCKET=self.base_context.get_preproc_bucket().bucket_name,
            S3_OUTPUT=self.build_context.s3_url(self.base_context.get_interim_bucket(),
                                                self.data_prefix),
            S3_TEMP=self.base_context.get_raw_bucket().s3_url_for_object("temp"),
            UPDATE_LOCATION=sf.JsonPath.string_at("$.update_location"),
            TABLE_NAME=sf.JsonPath.string_at("$.table_name"))

        # crawl relationalized tables
        if not custom_interim_crawler:
            self.interim_crawler = Crawler(self, "interimCrawler", build_context, data_prefix, base_context,
                                           databases.interim_database.database_input.name,
                                           self.base_context.get_interim_bucket())
        else:
            self.interim_crawler = custom_interim_crawler
        self.definition = self.definition.next(self.interim.definition.add_catch(
            handler=self.status_function, result_path="$.errors"))

        self.interim_crawler.set_log(log)

        success = sft.SnsPublish(
            self, "sns", topic=sns_config.ingested_done_topic,
            message=sf.TaskInput.from_object({"Source": self.data_prefix}))

        # If crawler fails another may be running so succeed
        self.interim_crawler.set_status_function(success)

        self.set_custom_definition(self.definition.next(self.interim_crawler.completed(success)))

    def build_stepfunction(self):
        super().build_stepfunction()
        self.set_function_path("launch_hudi_step")
        self.function_trigger([self.base_context.get_raw_bucket()])
