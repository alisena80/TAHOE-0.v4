from typing import List
from constructs import Construct
from aws_cdk import Duration
import aws_cdk.aws_glue as glue
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_stepfunctions as sf

from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct


class Crawler(TahoePipelineConstruct):
    """
    A class to represent a tahoe configured crawler

    Attributes
    ----------
    name  : str
        name of the state machine
    crawler  : glue.CfnCrawler
        glue crawler with defaults
    id  : str
        id of Crawler 
    definition  : Chain
        step function steps default configuration for crawler
    log : bool
        Include datasource logging for crawl operations

    Methods
    ----------
    completed(self, completed)
    """

    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context, target_database: str,
                 target_bucket: s3.Bucket, *, classifiers: List[str] = None, target=None, log=False):
        """
        Constructs crawler for tahoe context

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            data_prefix: str
                data prefix to name/describe
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            target_database : str
                output database to place schema in
            target_bucket: s3.Bucket
                target input bucket to crawl
            classifiers: List[str] 
                classifiers to add to crawler (Default: None)
        """
        super().__init__(scope, id, build_context, base_context, data_prefix)
        self.name = self.build_context.glue_name(self.data_prefix, id)
        self.id = id
        self.log = log

        default_target = glue.CfnCrawler.TargetsProperty(s3_targets=[glue.CfnCrawler.S3TargetProperty(
                                                         path=self.build_context.s3_url(target_bucket,
                                                                                        self.data_prefix))])

        self.crawler = glue.CfnCrawler(self, self.name,
                                       name=self.name,
                                       classifiers=classifiers,
                                       database_name=target_database,
                                       targets=default_target if target is None else target,
                                       role=self.base_context.get_pipeline_role().role_arn,
                                       table_prefix=self.build_context.get_stack_prefix())

        self.run_crawler = sft.LambdaInvoke(self, id + "crawlerRun",
                                            lambda_function=self.base_context.functions.crawler_run_function,
                                            payload=sf.TaskInput.from_object(
                                                {"crawler_name": self.crawler.name, "input.$": "$"}),
                                            result_path="$.input", payload_response_only=True)

        self.status = sft.LambdaInvoke(self, id + "crawlerStatus",
                                       lambda_function=self.base_context.functions.crawler_status_function,
                                       payload=sf.TaskInput.from_object(
                                           {"crawler_name": self.crawler.name, "input.$": "$.input"}),
                                       result_path="$.input", payload_response_only=True)

        self.wait = sf.Wait(self, id + "wait",
                            time=sf.WaitTime.seconds_path("$.input.estimatedTime"))
        self.definition = None
        self.crawl_status = self.base_context.functions.pipeline_database_task(self, id, "CRAWLED", self.data_prefix)

    def completed(self, completed: sf.IChainable):
        """
        Specifies next step function steps to take after crawler has completed running.
        Modifies and returns self.definition.

        Parameters
        ----------
        completed: sf.IChainable
            Next steps to step function

        """
        self.definition = self.run_crawler.add_retry(
            interval=Duration.minutes(5),
            errors=["CrawlerRunningException"]).add_catch(
            handler=self.status_function, result_path="$.errors").next(
            self.wait).next(
            self.status.add_catch(handler=self.status_function, result_path="$.errors"))
        check_result = sf.Choice(self, self.id + "choice")
        if self.log:
            completed = self.crawl_status.next(completed)
        # Step function controls whether to wait more or continue to next steps
        self.definition.next(check_result
                             .when(sf.Condition.string_equals("$.input.crawler_status", "PENDING"), self.wait)
                             .when(sf.Condition.string_equals("$.input.crawler_status", "COMPLETE"), completed))
        return self.definition

    def set_log(self, value: bool):
        """
        Sets whether to log the crawler on completion. Default is not to log.

        Parameters
        ----------
        value: bool
            Value to set log

        """
        self.log = value

    def set_status_function(self, function: sft.LambdaInvoke):
        """
        Sets the status function to be used for crawler failures

        Paramters
        ----------
        function: sft.LambdaInvoke
            Value to set function
        """

        self.status_function = function
