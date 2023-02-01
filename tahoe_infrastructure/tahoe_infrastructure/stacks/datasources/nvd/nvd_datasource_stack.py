from stacks.datasources.shared.interim_step_function import InterimStepFunction
from stacks.datasources.shared.retrieval_step_function import RetrievalStepFunction
from stacks.shared.constructs.crawler import Crawler
from stacks.shared.constructs.pyshell_job import Pyshell
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack


class NvdDatasourceInfrastructureStack(TahoeDatasourceNestedStack):

    def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix, build_context, base_context, log) -> None:
        super().__init__(scope, construct_id, build_context,
                         data_prefix, base_context, log)
        
        self.logger.info("Compiling nvd datasource")
        custom_raw_crawler = Crawler(self, "rawCrawler", build_context, data_prefix, base_context,
                                     self.etl_databases_config.raw_database.database_input.name,
                                     self.base_context.get_preproc_bucket(),
                                     classifiers=[self.base_context.get_json_array_classifier()])

        # bump dpu to one to support larger dataset
        custom_preprocess = Pyshell(self, "preprocessFn",
                                    build_context, data_prefix, base_context, "preprocess", self.sub_path,
                                    sub_folder="validation",
                                    dpu=1,
                                    DATASOURCE=data_prefix, S3_RAW=self.base_context.get_raw_bucket().bucket_name,
                                    S3_INTERIM=self.base_context.get_interim_bucket().bucket_name,
                                    S3_PREPROC=self.base_context.get_preproc_bucket().bucket_name)

        InterimStepFunction(self, "interim", self.build_context, self.data_prefix, self.base_context,
                            self.sns_config, self.etl_databases_config, self.sub_path,
                            custom_raw_crawler=custom_raw_crawler,
                            custom_preprocess=custom_preprocess,
                            log=self.log, validate=True, email_on_success=False).build_stepfunction()

        RetrievalStepFunction(self, "retrieval", build_context, data_prefix,
                              base_context, self.sns_config, self.sub_path,
                              url="https://nvd.nist.gov/feeds/json/cve/1.1/",
                              dpu=1,
                              email_on_success=False,
                              log=self.log).build_stepfunction()
