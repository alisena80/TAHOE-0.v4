from aws_cdk import RemovalPolicy
import aws_cdk.aws_dynamodb as dynamo
from constructs import Construct
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig


class BaseDynamo(TahoeConstruct):
    def __init__(self, scope: Construct, id: str, build_context: BuildConfig):
        super().__init__(scope, id, build_context=build_context)
        self.logging_dynamo = dynamo.Table(
            self, "loggingTable", table_name=f"{self.build_context.get_stack_prefix()}Logging", read_capacity=1,
            write_capacity=1, partition_key=dynamo.Attribute(name="PK", type=dynamo.AttributeType.STRING),
            sort_key=dynamo.Attribute(name="SK", type=dynamo.AttributeType.STRING), removal_policy=RemovalPolicy.DESTROY)