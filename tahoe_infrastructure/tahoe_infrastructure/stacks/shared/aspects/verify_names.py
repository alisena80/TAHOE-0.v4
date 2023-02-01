from aws_cdk import IAspect, Names
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesis as kinesis
import aws_cdk.aws_logs as logs
import jsii
from stacks.shared.lib.build_config import BuildConfig


@jsii.implements(IAspect)
class VerifyNames():
    def __init__(self, build_context: BuildConfig):
        self.build_context = build_context

    def visit(self, node):
        '''
        Visits all nodes in stack which 

        node
            root node
        '''
        
        if isinstance(node, iam.CfnPolicy):
            # Convert to Managed policy
            node.add_property_override("ManagedPolicyName", node.policy_name)
            node.add_override("Type", "AWS::IAM::ManagedPolicy")
            node.add_property_deletion_override("PolicyName")
            # verify name length and add name if no name exists
            if "LLNL_User_Policies" not in node.policy_name:
                node.add_property_override("ManagedPolicyName", self.build_context.policy_name(
                    self.truncate(Names.unique_id(node))))
            if len(node.policy_name) > 64:
                node.add_property_override(
                    "ManagedPolicyName", node.policy_name[0:64])
        if isinstance(node, iam.CfnRole):
            # add LLNL_User_Role if it doesn't ecist
            if "LLNL_User_Role" not in node.role_name:
                node.add_property_override("RoleName", self.build_context.role_name(
                    self.truncate(Names.unique_id(node))))
            if len(node.role_name) > 64:
                node.add_property_override("RoleName", node.role_name[0:64])
        if isinstance(node, kinesis.CfnStream):
            node.add_property_override("StreamModeDetails.StreamMode", "PROVISIONED")
            node.add_property_deletion_override("StreamModeDetails")
        if isinstance(node, logs.CfnLogGroup):
            node.add_property_deletion_override('Tags')
    
    def truncate(self, target: str):
        '''
        Truncates string to stay within 64 char limit

        target: str

        '''
        return target[len(self.build_context.get_stack_prefix()):len(self.build_context.get_stack_prefix()) + 64-len(self.build_context.policy_name(""))]
