from aws_cdk import Duration
from constructs import Construct
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_lambda as l
from .base_streams import BaseStreams
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_sns as sns


class BaseStepFunctions(TahoeConstruct):
    """
    Step functions in the base stack.

    Attributes
    ----------
    query_sns_topic : sns.Topic
        Sns topic
    base_role : iam.Role
        Role to be used to run all pipeline operations
    query_state_machine : sf.StepFunction
        State machine to dictate flow of queries
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, stream: BaseStreams,
                 query_sns_topic: sns.Topic = None, base_role=None):
        super().__init__(scope, id, build_context)
        self.query_sns_topic = query_sns_topic
        self.base_role = base_role
        # create stepfunction to handle query api
        self.create_query_step_function(stream)

    def create_query_step_function(self, stream) -> None:
        """
        Creates the query step function
        """
        name = self.build_context.sf_name("query", "helper")
        # Prepares input
        self.input_function = l.Function(self, "payloadFunction",
                                         code=l.Code.from_asset(
                                             "../base/functions/prepare_query_input"),
                                         runtime=l.Runtime.PYTHON_3_9,
                                         handler="function.handler",
                                         role=self.base_role,
                                         timeout=Duration.seconds(120), layers=[self.build_context.get_logging_layer()])

        reformat_function_input = sft.LambdaInvoke(self, "payloadFunctionInvoke",
                                                   lambda_function=self.input_function,
                                                   payload_response_only=True)

        map = sf.Map(self, "Map",
                     max_concurrency=0,
                     items_path=sf.JsonPath.string_at("$.queries"),
                     parameters={"data": sf.JsonPath.string_at('$$.Map.Item.Value')},
                     result_path=sf.JsonPath.string_at("$.output"))

        cache_or_query = sf.Choice(self, "cacheOrQuery")

        cache_pass = sf.Pass(self, "passCache", parameters=self.query_sf_parameters())

        query_step_function = sft.StepFunctionsStartExecution(self, "execSf",
                                                              state_machine=sf.StateMachine.from_state_machine_arn(
                                                                  self, "sfId",
                                                                  state_machine_arn=sf.JsonPath.string_at(
                                                                      "$.data.arn")),
                                                              integration_pattern=sf.IntegrationPattern.RUN_JOB,
                                                              output_path="$.Output")

        # Decide whether to pass cache or query again
        cache_or_query.when(sf.Condition.is_not_present("$.data.cache"), query_step_function) \
            .when(sf.Condition.is_present("$.data.cache"), cache_pass)

        # map iterator to run step functions in parallel
        map.iterator(cache_or_query)

        # Task to format payload for further consumer processing
        reformat_function = l.Function(self, "inputFunction",
                                       code=l.Code.from_asset(
                                           "../base/functions/convert_query_output"),
                                       runtime=l.Runtime.PYTHON_3_9,
                                       handler="function.handler",
                                       role=self.base_role,
                                       timeout=Duration.seconds(120), layers=[self.build_context.get_logging_layer()])

        reformat_function_task = sft.LambdaInvoke(self, "inputFunctionInvoke",
                                                  lambda_function=reformat_function,
                                                  payload_response_only=True,
                                                  result_path="$.output",
                                                  payload=sf.TaskInput.from_object({"output.$": "$.output"}))

        stream.add_filter("reformatFilter", reformat_function.log_group)
        stream.add_filter("inputFilter", self.input_function.log_group)
        # Publishes to sns for consumer
        sns_publish = sft.SnsPublish(
            self, "snsPublishToConsumer", message=sf.TaskInput.from_json_path_at("$.output"),
            message_attributes={"Consumer": sft.MessageAttribute(
                value=sf.JsonPath.string_at("$.report-type"),
                data_type=sft.MessageAttributeDataType.STRING)},
            topic=self.query_sns_topic)

        map = reformat_function_input.next(map.next(reformat_function_task.next(sns_publish)))

        self.query_state_machine = sf.StateMachine(
            self, "queryStateMachine", state_machine_name=name, definition=map, role=self.base_role)

    def query_sf_parameters(self):
        """
        Output of individual query functions

        :return: Output of step function
        """
        return {"Source.$": "$.data.type",
                "Details.$": "$.data.additional_details",
                "Query.$": "$.data.query",
                "Execution.$": "$$.Execution.Id"}

    def input_function_add_parameter(self, key, value):
        """
        Adds env variable to api lambda function

        key: str
            Key for lambda env variable
        val: str
            Value for lambda env variable
        """
        self.input_function.add_environment(key, value)
