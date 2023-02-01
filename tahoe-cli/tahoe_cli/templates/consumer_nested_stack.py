
from stacks.shared.constructs.tahoe_consumer_nested_stack import TahoeConsumerNestedStack
from stacks.shared.constructs.tahoe_construct import TahoeConstruct


class {{name}}ConsumerInfrastructureStack(TahoeConsumerNestedStack):
   def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix, build_context, base_context, datasources) -> None:
      super().__init__(scope, construct_id, build_context,
                           data_prefix, base_context)

      self.logger.info("Compiling {{name}} datasource")
