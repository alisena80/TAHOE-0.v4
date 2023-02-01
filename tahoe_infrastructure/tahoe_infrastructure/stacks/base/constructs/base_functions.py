from aws_cdk import Duration
from constructs import Construct
import aws_cdk.aws_lambda as l
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_lambda_event_sources as l_event
from stacks.base.constructs.base_dynamo import BaseDynamo
from stacks.base.constructs.base_streams import BaseStreams

from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig

import os


class BaseFunctions(TahoeConstruct):
    """
    A class to represent an encapsulation of all base functions to enable 
    sharing between all datasources/consumers.

    Attributes
    ----------
    crawler_run_function : lambda.Function
        Starts crawler run. "crawler_name" is specified from step function event
    crawler_status_function: lambda.Function
        Checks status of crawler. "crawler_name" is specified from step function event
    pipeline_status_function: lambda.Function
        Tracks errors in pipeline
    pipeline_database_function: lambda.Function
        Source health reporting function
    human_approval_function: lambda.Function
        Human approval function to enable reactivating frozen tasks. TODO Move code to api



    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig,
                 base_role: iam.Role, email_sns_topic, stream: BaseStreams, dynamo: BaseDynamo):
        """
        Constructs all base functions

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
        """
        super().__init__(scope, id, build_context)

        self.crawler_run_function = l.Function(self, "crawlerRun", runtime=l.Runtime.PYTHON_3_9,
                                               handler="function.handler",
                                               code=l.Code.from_asset(
                                                   os.path.join("../base", "functions", "crawler_start")),
                                               role=base_role, timeout=Duration.seconds(120),
                                               layers=[self.build_context.get_logging_layer()])
        stream.add_filter("crawlerRunFilter", self.crawler_run_function.log_group)
        self.crawler_status_function = l.Function(self, "crawlerStatus", runtime=l.Runtime.PYTHON_3_9,
                                                  handler="function.handler",
                                                  code=l.Code.from_asset(
                                                      os.path.join("../base", "functions", "crawler_status")),
                                                  role=base_role, timeout=Duration.seconds(120),
                                                  layers=[self.build_context.get_logging_layer()])
        stream.add_filter("crawlerStatusFilter", self.crawler_status_function.log_group)

        self.pipeline_status_function = l.Function(
            self, "pipelineStatus", runtime=l.Runtime.PYTHON_3_9, handler="function.handler", code=l.Code.from_asset(
                os.path.join("../base", "functions", "report_datasource_status")),
            role=base_role, timeout=Duration.seconds(120),
            layers=[self.build_context.get_logging_layer()],
            environment={"SNS_TOPIC": email_sns_topic.topic_arn})

        self.pipeline_database_function = l.Function(self, "pipelineDatabase", runtime=l.Runtime.PYTHON_3_9,
                                                     handler="function.handler",
                                                     code=l.Code.from_asset(
                                                         os.path.join("../base", "functions", "track_datasource")),
                                                     role=base_role, timeout=Duration.seconds(
                                                         120),
                                                     environment={"SNS_TOPIC": email_sns_topic.topic_arn},
                                                     layers=[self.build_context.get_logging_layer()])

        stream.add_filter("pipelinedbStatusFilter", self.pipeline_database_function.log_group)

        self.run_redshift_command_function = l.Function(
            self, "redshiftCommand", runtime=l.Runtime.PYTHON_3_9, handler="function.handler", code=l.Code.from_asset(
                os.path.join("../base", "functions", "run_redshift_command")),
            role=base_role, timeout=Duration.seconds(120),
            environment={"REDSHIFT_SECRET_NAME": self.build_context.get_credential("tahoe-redshift-secret-name")})

        self.pipeline_success_notification_function = l.Function(self, "pipelineSuccessNotification",
                                                                 runtime=l.Runtime.PYTHON_3_9,
                                                                 handler="function.handler",
                                                                 code=l.Code.from_asset(
                                                                     os.path.join("../base", "functions",
                                                                                  "report_pipeline_success")),
                                                                 role=base_role, timeout=Duration.seconds(120),
                                                                 environment={"SNS_TOPIC": email_sns_topic.topic_arn},
                                                                 layers=[self.build_context.get_logging_layer()])

        self.human_approval_function = l.Function(self, "humanApproval", runtime=l.Runtime.PYTHON_3_9,
                                                  handler="function.handler",
                                                  code=l.Code.from_asset(
                                                      os.path.join("../base", "functions", "send_task_token")),
                                                  role=base_role, timeout=Duration.seconds(120),
                                                  layers=[self.build_context.get_logging_layer()])
        self.kinesis_consumer = l.Function(
            self, "kinesisConsumer", runtime=l.Runtime.PYTHON_3_9, handler="function.handler", code=l.Code.from_asset(
                os.path.join("../base", "functions", "kinesis_consumer")),
            role=base_role, timeout=Duration.seconds(120),
            layers=[self.build_context.get_logging_layer()])

        self.kinesis_consumer.add_environment("TABLE_NAME", dynamo.logging_dynamo.table_name)
        self.kinesis_consumer.add_environment("DEFAULT_SNS", email_sns_topic.topic_arn)

        self.kinesis_consumer.add_event_source(l_event.KinesisEventSource(
            stream.log_stream, starting_position=l.StartingPosition.TRIM_HORIZON, retry_attempts=1))

    def pipeline_status_task(self, scope, id, data_prefix):

        # Status can take both failed and succeeded jobs, so send the base "$" instead
        # since we don't know if $.error or $.input will be there
        return sft.LambdaInvoke(scope, id + "Status", lambda_function=self.pipeline_status_function,
                                payload=sf.TaskInput.from_object({"details": sf.JsonPath.string_at("$"),
                                                                  "dataPrefix": data_prefix}),
                                result_path="$.input", payload_response_only=True)

    def pipeline_database_task(self, scope, id, status, data_prefix) -> sft.LambdaInvoke:
        """
        Constructs Lambda invoke task for pipeline_database_function to be used in step functions

        Parameters
        ----------
            scope : Construct
                construct scope
            id: str
                construct id
            status: str
                Status to pass into database object
            data_prefix: str
                data prefix to build invoke for
        """
        return sft.LambdaInvoke(scope, id + "database", lambda_function=self.pipeline_database_function,
                                payload=sf.TaskInput.from_object({"status": status, "dataPrefix": data_prefix,
                                                                  "input": sf.JsonPath.string_at("$.input")}),
                                result_path=sf.JsonPath.DISCARD, payload_response_only=True)

    def pipeline_database_wait_task(self, scope, id, status, data_prefix) -> sft.LambdaInvoke:
        """
        Constructs Lambda invoke task for pipeline_database_function to be used in step functions with human approval
        wait

        Parameters
        ----------
            scope : Construct
                construct scope
            id: str
                construct id
            status: str
                Status to pass into database object
            data_prefix: str
                data prefix to build invoke for

        """
        return sft.LambdaInvoke(scope, id + "databaseWait", timeout=Duration.seconds(200),
                                lambda_function=self.pipeline_database_function,
                                integration_pattern=sf.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
                                payload=sf.TaskInput.from_object({"status": status, "dataPrefix": data_prefix,
                                                                  "input": sf.JsonPath.string_at("$.input"),
                                                                  "taskToken": sf.JsonPath.task_token}),
                                result_path="$.input")

    def pipeline_success_notification_task(self, scope, id, status, data_prefix) -> sft.LambdaInvoke:
        return sft.LambdaInvoke(scope, id, timeout=Duration.seconds(200),
                                lambda_function=self.pipeline_success_notification_function,
                                payload=sf.TaskInput.from_object({"status": status, "dataPrefix": data_prefix,
                                                                  "input": sf.JsonPath.string_at("$.input")}),
                                result_path=sf.JsonPath.DISCARD, payload_response_only=True)

    def run_redshift_command_task(self, scope, id, sql_query) -> sft.LambdaInvoke:
        return sft.LambdaInvoke(scope, id, timeout=Duration.seconds(200),
                                lambda_function=self.run_redshift_command_function,
                                payload=sf.TaskInput.from_object({"sql_query": sql_query}),
                                result_path=sf.JsonPath.DISCARD, payload_response_only=True)
