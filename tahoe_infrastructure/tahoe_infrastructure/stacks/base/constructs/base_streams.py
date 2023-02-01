from constructs import Construct
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig
import aws_cdk.aws_kinesis as kinesis
import aws_cdk.aws_logs as logs
import aws_cdk.aws_logs_destinations as log_destination
import aws_cdk.aws_iam as iam


class BaseStreams(TahoeConstruct):
    """
    Base Kinesis Streams.

    Attributes
    ----------
        log_stream: kinesis.Stream
            Log kinesis stream
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, base_role: iam.Role):
        """
        Create base Kinesis streams.

        Parameters
        ----------
            scope: Construct
                Construct scope
            id: str
                Logical id
            build_context: BuildConfig
                Build context for deployment
            base_role: iam.Role
                Base role

        """
        super().__init__(scope, id, build_context)
        self.base_role = base_role

        self.log_stream = kinesis.Stream(self, "logStream", stream_mode=kinesis.StreamMode.PROVISIONED)

    def add_filter(self, name: str, log_group: logs.LogGroup, construct=None):
        """

        Add subscription filter.

        Parameters
        ----------
            name: str
                Logical id and name of the subscription
            log_group: logs.LogGroup
                Log group

        """
        if construct is None:
            construct = self
        logs.SubscriptionFilter(construct, name, log_group=log_group, filter_pattern=logs.FilterPattern.any_term_group(
            [f"DBLOG"],
            [f"NOTIFY"]),
            destination=log_destination.KinesisDestination(
            stream=self.log_stream, role=self.base_role))
