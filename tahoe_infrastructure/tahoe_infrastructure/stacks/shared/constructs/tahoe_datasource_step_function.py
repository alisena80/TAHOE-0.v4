from constructs import Construct
from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct
from typing import List, Mapping, Union
import os
from aws_cdk import Duration, aws_stepfunctions as sf
from aws_cdk import aws_lambda as l
from aws_cdk import aws_sns as sns
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_lambda_event_sources as lambda_event
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as event_targets
from aws_cdk import aws_sqs as sqs


class TahoeStepFunctionConstruct(TahoePipelineConstruct):
    """
    Tahoe step function definition.

    Attributes
    ----------
        id: str
            Construct id
        status_function: sft.LambdaInvoke
            Status function invoke on error
        definition: sf.Chain
            State machine definition
        function: lambda.Function
            Lambda function that triggers step function if applicable
        rule: events.Rule
            Rule that triggers step function if applicable
    """

    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path):
        '''
        Creates step function

        Parameters
        ----------
            scope : Construct
                construct scope
            construct_id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            data_prefix: str
                data prefix to name/describe
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed

        '''
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path)
        self.id = id

        self.logger.debug(str(self.base_context.__dict__))
        self.tahoe_commons = self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_tahoe_common())

        self.pydeequ_zip = self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_pydeequ_zip())

        self.pydeequ_jar = self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_pydeequ_jar())

        self.hudi_jars = [self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_hudi_jar())]

        # Status function lambda invocation one per step function. All failures route through this step
        self.status_function = self.base_context.functions.pipeline_status_task(scope, id, data_prefix)
        self.definition = sf.Pass(self, "Start")
        self.function = None
        self.function_path = None
        self.has_custom_definition = False
        self.rule = None
        self.create_logger()

    def set_custom_definition(self, definition: sf.Chain):
        """
        Set definition of step function contruct.

        Parameters
        ----------
        definition: sf.Chain
            State machine definition
        """
        self.has_custom_definition = True
        self.definition = definition

    def build_stepfunction(self):
        """Build a step function with definition."""
        if not self.has_custom_definition:
            raise ValueError(f"{self.id} {self.data_prefix} expects to have a custom definiton")
        self.name = self.build_context.sf_name(self.data_prefix, self.id)
        self.state_machine = sf.StateMachine(
            self, self.name, state_machine_name=self.name, definition=self.definition,
            role=self.base_context.get_pipeline_role())

    def set_function_path(self, name):
        """
        Set lambda to trigger this step function. By default launch_interim_step is used for interim stacks and launch_consumer_step is used for consumers.

        Parameters
        ----------
            name: str
                name of the lamdba function
        """
        self.function_path = name

    def function_trigger(
            self, event_sources: List[Union[sns.Topic, s3.Bucket]],
            filter: Mapping[str, sns.SubscriptionFilter] = None):
        """
        Add function to trigger step function.

        Parameters
        ----------
            source_type: str
                the source is a consumer or datasource. Must have value of "consumer" or  "datasource". Identifies which function to use.
            event_sources: List[sns.Topic]
                sources to trigger step function
            filter: Mapping[str, sns.SubscriptionFilter]
                filter attached to sns to limit function triggers to certain message attributtes. Default None.
  
        """
        os.chdir("../base")

        if self.function_path is None and "datasources" in self.name_from_sub_path():
            self.function_path = "launch_interim_step"
        elif self.function_path is None and "consumers" in self.name_from_sub_path():
            self.function_path = "launch_consumer_step"
        elif self.function_path is None:
            raise ValueError("Source type must have a function path")

        asset = l.Code.from_asset(os.path.join(os.getcwd(), "functions", self.function_path))

        self.function = l.Function(self, "function", runtime=l.Runtime.PYTHON_3_9,
                                   handler="function.handler",
                                   code=asset,
                                   role=self.base_context.get_pipeline_role(),
                                   layers=[self.build_context.get_logging_layer()])
        self.function.add_environment(
            "STEP_FUNCTION", self.state_machine.state_machine_arn)
        self.function.add_environment(
            "DATA_PREFIX", self.data_prefix)
        for event in event_sources:
            if type(event) == sns.Topic:
                self.function.add_event_source(
                    lambda_event.SnsEventSource(event, filter_policy=filter))
            if type(event) == s3.Bucket:

                # if s3 bucket then create queue and attach to bucket and function as a storage layer
                queue = sqs.Queue(self, "queue")
                event.add_event_notification(
                    s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue),
                    s3.NotificationKeyFilter(prefix=self.data_prefix))
                self.function.add_environment("QUEUE_URL", queue.queue_url)
                self.rule = events.Rule(self, "timer", description="Triggers lambda function {}".format(self.name),
                                        schedule=events.Schedule.rate(Duration.minutes(15)), targets=[
                    event_targets.LambdaFunction(self.function)])

    def timer_trigger(self, duration: events.Schedule):
        """
        Add event bridge schedule to trigger step function.

        Parameters
        ----------
            duration: events.Schedule
                Schedule to trigger step function on
        """
        self.rule = events.Rule(self, "timer", description="Triggers step function {}".format(self.name),
                                schedule=duration, targets=[
            event_targets.SfnStateMachine(self.state_machine, role=self.base_context.get_pipeline_role())])

    def api_trigger(self):
        """
        Adds trigger to step function by api controller TODO

        Parameters
        ----------
            api_path: str
                api path 
        """
        pass
