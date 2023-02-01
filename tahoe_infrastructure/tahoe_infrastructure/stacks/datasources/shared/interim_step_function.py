from constructs import Construct
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sft
from stacks.shared.constructs.crawler import Crawler
from stacks.shared.constructs.etl_database_config import EtlDatabaseConfig
from stacks.shared.constructs.etl_job import Etl
from stacks.shared.constructs.pyshell_job import Pyshell
from stacks.shared.constructs.sns_config import SnsConfig
from stacks.shared.constructs.tahoe_datasource_step_function import TahoeStepFunctionConstruct


class InterimStepFunction(TahoeStepFunctionConstruct):
    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context, sns_config: SnsConfig,
                 databases: EtlDatabaseConfig, sub_path, *, preprocess=True, validate=False,
                 custom_raw_crawler: Crawler = None,
                 custom_interim_crawler: Crawler = None, custom_etl: Etl = None, custom_preprocess: Pyshell = None,
                 log=True,
                 custom_validate: Etl = None, email_on_success=False):
        """
        Construct interim step function with crawling/validation/preprocessing.

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
            sns_config: SnsConfig
                Sns configuration created by the nested stack
            databases: EtlDatabasConfig
                Permissions and databasd created by nesteds stack
            sub_path: str
                base path of file [datasources,base,consumer]
            preprocess: bool
                Enable default preproccess ETL 
            validate :bool
                Enable default validation ETL
            log: bool
                track status of the pipeline
            custom_etl: Etl
                Custom Etl definition 
            custom_preprocess: Etl/Pyshell
                Custom preprocess definition 
            custom_validate: Etl/Pyshell
                Custom validate definiton
            custom_raw_crawler: Crawler
                Custom raw crawler required if custom classifiers/locations
            custom_interim_crawler: Crawler
                Custom interim crawler required if custom classifiers/locations
        """
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        self.sns_config = sns_config

        extra_python_files = [self.tahoe_commons, self.pydeequ_zip]

        if not custom_etl:
            self.transform_to_interim = Etl(self, "etlInterim", build_context, data_prefix, base_context,
                                            "do_transform", self.sub_path,
                                            jars=[self.pydeequ_jar],
                                            python_files=extra_python_files,
                                            DATABASE=databases.raw_database.database_input.name,
                                            S3_OUTPUT=self.build_context.s3_url(self.base_context.get_interim_bucket(),
                                                                                self.data_prefix),
                                            STACK_PREFIX=self.build_context.get_stack_prefix(),
                                            S3_TEMP=self.base_context.get_raw_bucket().s3_url_for_object("temp"))
        else:
            self.transform_to_interim = custom_etl

        # Raw crawl data from preprocess bucket
        if not custom_raw_crawler:
            self.raw_crawler = Crawler(self, "rawCrawlers", build_context, data_prefix, base_context,
                                       databases.raw_database.database_input.name,
                                       self.base_context.get_preproc_bucket())
        else:
            self.raw_crawler = custom_raw_crawler

        # Interim Crawl data after relationalization from interim bucket
        if not custom_interim_crawler:
            self.interim_crawler = Crawler(self, "interimCrawler", build_context, data_prefix, base_context,
                                           databases.interim_database.database_input.name,
                                           self.base_context.get_interim_bucket())
        else:
            self.interim_crawler = custom_interim_crawler
        # preprocess pyshell if required to properly crawl data from Glue
        if preprocess or custom_preprocess:
            if custom_preprocess:
                self.preproc = custom_preprocess
            else:
                self.preproc = Pyshell(self, "preprocessFn",
                                       build_context, data_prefix, base_context, "preprocess", self.sub_path,
                                       sub_folder="validation",
                                       DATASOURCE=data_prefix, S3_RAW=self.base_context.get_raw_bucket().bucket_name,
                                       S3_INTERIM=self.base_context.get_interim_bucket().bucket_name,
                                       S3_PREPROC=self.base_context.get_preproc_bucket().bucket_name)
            # add preprocess and error handling to the step function chain
            self.definition = self.definition.next(
                self.preproc.definition.add_catch(handler=self.status_function, result_path="$.errors"))
            if log:
                # Add log step
                self.definition = self.definition.next(
                    self.base_context.functions.pipeline_database_task(self, "preprocess", "PREPROCESSED",
                                                                       self.data_prefix))

        # Validate schema etl job
        if validate or custom_validate:
            if custom_validate:
                self.validate = custom_validate
            else:
                self.validate = Etl(self, "validationFn",
                                    build_context, data_prefix, base_context, "validate", self.sub_path,
                                    sub_folder="validation",
                                    DATABASE=databases.raw_database.database_input.name,
                                    python_modules=["deepdiff"],
                                    python_files=[self.tahoe_commons])
            if log:
                # Create log states and wait log for failed schema creates
                log_validate = self.base_context.functions.pipeline_database_task(self, "validated", "VALIDATED",
                                                                                  self.data_prefix)

                self.validate.definition = self.validate.definition.next(
                    log_validate)
              # Wait for human approval if validation fails
            log_approval = \
                self.base_context.functions.pipeline_database_wait_task(self, "approval", "FROZEN",
                                                                        self.data_prefix).add_catch(
                    errors=["States.TaskFailed,States.Timeout"], handler=self.status_function,
                    result_path="$.errors")

            # Use log states if required
            self.validate.definition = self.validate.definition.add_catch(handler=log_approval,
                                                                          result_path="$.input")
            self.definition = self.definition.next(self.validate.definition)
        success = sft.SnsPublish(
            self, "sns", topic=sns_config.ingested_done_topic,
            message=sf.TaskInput.from_object({"Source": self.data_prefix}))

        # If success emails are requested, add them here
        if email_on_success:
            success = self.base_context.functions.pipeline_success_notification_task(self, "success_notification",
                                                                                     "success_notification",
                                                                                     self.data_prefix).next(success)

        # set the log value to determine if logging is enabled
        self.interim_crawler.set_log(log)
        self.raw_crawler.set_log(log)
        self.raw_crawler.set_status_function(self.status_function)
        self.interim_crawler.set_status_function(self.status_function)
        self.set_custom_definition(self.definition.next(self.raw_crawler.completed(
            self.transform_to_interim.definition.add_catch(handler=self.status_function, result_path="$.errors").next(
                self.interim_crawler.completed(success)))))

    def build_stepfunction(self):
        super().build_stepfunction()
        self.function_trigger([self.sns_config.download_done_topic])
