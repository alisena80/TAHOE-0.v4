import resource
from sys import prefix
from constructs import Construct
import aws_cdk.aws_iam as iam
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig
from aws_cdk import Stack
import boto3


class BaseRoles(TahoeConstruct):
    """

    A class to represent an encapsulation of all the base roles to enable pipeline operations.

    Attributes
    ----------
    base_role : iam.Role
        Role to be used to run all pipeline operations
    base_policy : iam.Policy
        Policy used in base role
    """

    def __init__(
            self, scope: Construct, id: str, build_context: BuildConfig,
            kms_policy: iam.Policy):
        """

        Construct all base roles.

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            kms_policy : iam.Policy
                kms policy to attach to base roles for decode
        """
        super().__init__(scope, id, build_context)

        # Use modiified LLNL permisssions boundary if in the gov cloud.
        if "gov" in self.build_context.get_region():
            boundry = iam.ManagedPolicy.from_managed_policy_arn(
                self, "permissionBoundary",
                "arn:{}:iam::{}:policy/default-modified-permissions-boundary".
                format(Stack.of(self).partition, Stack.of(self).account))
        else:
            boundry = None

        base_role_name = self.build_context.role_name("Base_Role")

        # create pipeline execution role
        self.base_role = iam.Role(self, base_role_name,
                                  role_name=base_role_name,
                                  permissions_boundary=boundry,
                                  assumed_by=iam.CompositePrincipal(
                                      iam.ServicePrincipal(
                                          'lakeformation.amazonaws.com'),
                                      iam.ServicePrincipal(
                                          "glue.amazonaws.com"),
                                      iam.ServicePrincipal(
                                          "lambda.amazonaws.com"),
                                      iam.ServicePrincipal(
                                          "events.amazonaws.com"),
                                      iam.ServicePrincipal(
                                          "states.amazonaws.com"),
                                      iam.ServicePrincipal(
                                          "redshift.amazonaws.com"),
                                      iam.ServicePrincipal(
                                          "logs.amazonaws.com")
                                  ))
        # create pipeline execution policy
        self.base_policy = iam.Policy(
            self, "BasePolicy", policy_name=self.build_context.policy_name(
                "Base_Policy"))

        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["kinesis:*"],
                resources=["*"]))

        # invoke sns and controls flow of program
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["SNS:Publish", "SNS:Subscribe"],
                resources=[self.build_context.resource_arn("sns")]))

        # S3 access of code artifacts and datasource buckets
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["s3:*"],
            resources=[self.build_context.regionless_resource_arn(
                "s3", self.build_context.get_stack_prefix()),
                self.build_context.regionless_resource_arn(
                "s3", self.build_context.get_asset_bucket()),
                self.build_context.regionless_resource_arn(
                "s3", self.build_context.get_asset_bucket(
                    truncated=True),
                not_all=True)]))

        # Sqs permissions to stack prefix resources
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["sqs:*"],
                resources=[self.build_context.resource_arn("sqs")]))

        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["sqs:*"], resources=[self.build_context.resource_arn("sqs")]))

        # Glue permissions to run jobs and access catalog resources
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["glue:*"],
                resources=[self.build_context.resource_arn(
                    "glue", "crawler/"),
                    self.build_context.resource_arn(
                    "glue", "connection/*", ignore_prefix=True),
                    self.build_context.resource_arn("glue", "job/"),
                    self.build_context.resource_arn(
                    "glue", "database/"),
                    self.build_context.resource_arn(
                    "glue", "table/"),
                    self.build_context.resource_arn(
                    "glue", "catalog", ignore_prefix=True)]))

        # Glue permission required to glue:GetCrawlerMetrics which requires all resource permissions
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["glue:GetCrawlerMetrics"], resources=["*"]
        ))

        # Required for vpc execution of lambda/glue
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["ec2:DescribeSecurityGroups",
                     "ec2:DeleteNetworkInterface",
                     "ec2:DescribeVpcAttribute",
                     "ec2:CreateNetworkInterface",
                     "ec2:DescribeNetworkInterfaces",
                     "ec2:DescribeVpcEndpoints",
                     "ec2:DescribeSubnets",
                     "ec2:DescribeRouteTables"], resources=["*"]))

        # Required for glue execution
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["ec2:CreateTags", "ec2:DeleteTags"],
                resources=[
                    f"arn:{self.build_context.get_partition()}:ec2:*:*:network-interface/*",
                    f"arn:{self.build_context.get_partition()}:ec2:*:*:security-group/*",
                    f"arn:{self.build_context.get_partition()}:ec2:*:*:instance/*"],
                conditions={
                    "ForAllValues:StringEquals":
                    {"aws:TagKeys": ["aws-glue-service-resource"]}}))

        # handles logging permission for glue, lambda,  and api when activated
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream",
                         "logs:PutLogEvents"],
                resources=["*"]))

        # Allows calling step function from another step function required for query task
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["events:PutTargets", "events:PutRule",
                         "events:DescribeRule"],
                resources=[(
                    f"arn:{self.build_context.get_partition()}:events:"
                    f"{self.build_context.get_region()}:{self.build_context.get_account()}:"
                    f"rule/StepFunctionsGetEventsForStepFunctionsExecutionRule")]))

        # Lambda permissions to stack prefix resources
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["lambda:*"],
                resources=[self.build_context.resource_arn(
                    "lambda", "function:")]))

        # Databrew permissions to stack prefix resources
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["databrew:*"],
                resources=[self.build_context.resource_arn("databrew")]))

        # Step functions permissions to stack prefix resources
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["states:*"],
                resources=[self.build_context.resource_arn(
                    "states", "stateMachine:")]))

        # All lakeformation permissions
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["lakeformation:*"], resources=["*"]))

        # used for health status reporting (Not in stack)
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["dynamodb:*"], resources=["*"]))

        # needed to read sandbox stack secrets
        self.base_policy.add_statements(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue",
                     "secretsmanager:DescribeSecret"],
            resources=["*"],
            conditions={"StringEquals": {
                "aws:ResourceTag/aws:cloudformation:stack-name":
                "TahoeSandboxStack"
            }}
        ))

        # manage any secrets generated this stack
        self.base_policy.add_statements(
            iam.PolicyStatement(
                actions=["secretsmanager:*"],
                resources=[self.build_context.resource_arn("secretsmanager", "secret:"),
                           self.build_context.resource_arn(
                               "secretsmanager", "secret:opensearch_host", ignore_prefix=True)]))

        # attach policies to pipeline role
        kms_policy.attach_to_role(self.base_role)
        self.base_policy.attach_to_role(self.base_role)

        # Temporary permissions

        # to upload data into redshift and
        self.base_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonDocDBFullAccess'))

        # required for redshift access
        self.base_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonRedshiftAllCommandsFullAccess'))

        # Redshift data access
        self.base_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonRedshiftDataFullAccess'))

        # Don't allow cdk resources to add to the policy automatically
        self.base_role = self.base_role.without_policy_updates()

        # create user group and role for read only resource
        self.read_only_user_group, self.read_role = self.create_user_group(
            self.build_context.role_name("ReadOnly"))

    def create_user_group(self, name: str):
        """

        Create user group and read only permissions.

        Parameters
        ----------
            name: str
                Name of the role/user group
        """
        # Read only role that can be assumed
        read_role = iam.Role(
            self, name + 'Role', role_name=name,
            assumed_by=iam.AccountPrincipal(
                account_id=self.build_context.get_account()))

        # User group which gives users access to assume read only role
        group = iam.Group(self, name, group_name=name)

        group_assume_policy = iam.Policy(
            self, "AssumeReadRole", policy_name=self.build_context.policy_name("Assume_Read_Role"))

        group_assume_policy.add_statements(iam.PolicyStatement(
            actions=["sts:AssumeRole"], resources=[read_role.role_arn]))

        read_only_policy = iam.Policy(
            self, "ReadOnly", policy_name=self.build_context.policy_name(
                "Read_Only"))

        # Read permissions to see details of SNS with stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["sns:Check*", "sns:Get*", "sns:List*"],
                resources=[self.build_context.resource_arn("sns")]))

        # SNS permissions to see a list of all topics in console
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["SNS:ListTopics"],
                resources=[self.build_context.resource_arn(
                    "sns", "*", ignore_prefix=True)]))

        # Read permissions to see details of SQS with stack prefix
        # read_only_policy.add_statements(
        #     iam.PolicyStatement(
        #         actions=["sqs:Get*"],
        #         resources=[self.build_context.resource_arn("sqs")]))

        # Read
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["sqs:List*", "sqs:Get*"],
                resources=["*"]))

        # Glue read permissions for jobs, crawler, databases, catalog, tables with stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["glue:BatchGetWorkflows",
                         "glue:CheckSchemaVersionValidity",
                         "glue:GetCatalogImportStatus", "glue:GetClassifier",
                         "glue:GetClassifiers", "glue:GetCrawler",
                         "glue:GetConnections", "glue:GetCrawlers",
                         "glue:GetDatabase", "glue:GetDatabases",
                         "glue:GetDataCatalogEncryptionSettings",
                         "glue:GetDataflowGraph", "glue:GetDevEndpoint",
                         "glue:GetDevEndpoints", "glue:GetJob",
                         "glue:GetJobBookmark", "glue:GetJobRun",
                         "glue:GetJobRuns", "glue:GetJobs", "glue:GetMapping",
                         "glue:GetMLTaskRun", "glue:GetMLTaskRuns",
                         "glue:GetMLTransform", "glue:GetMLTransforms",
                         "glue:GetPartition", "glue:GetPartitions",
                         "glue:GetPlan", "glue:GetRegistry",
                         "glue:GetResourcePolicy", "glue:GetSchema",
                         "glue:GetSchemaByDefinition", "glue:GetSchemaVersion",
                         "glue:GetSchemaVersionsDiff",
                         "glue:GetSecurityConfiguration",
                         "glue:GetSecurityConfigurations", "glue:GetTable",
                         "glue:GetTables", "glue:GetTableVersion",
                         "glue:GetTableVersions", "glue:GetTags",
                         "glue:GetTrigger", "glue:GetTriggers",
                         "glue:GetUserDefinedFunction",
                         "glue:GetUserDefinedFunctions", "glue:GetWorkflow",
                         "glue:GetWorkflowRun",
                         "glue:GetWorkflowRunProperties",
                         "glue:GetWorkflowRuns",
                         "glue:QuerySchemaVersionMetadata"],
                resources=[self.build_context.resource_arn(
                    "glue", "crawler/"),
                    self.build_context.resource_arn(
                    "glue", "connection/*", ignore_prefix=True),
                    self.build_context.resource_arn("glue", "job/"),
                    self.build_context.resource_arn(
                    "glue", "database/"),
                    self.build_context.resource_arn(
                    "glue", "table/"),
                    self.build_context.resource_arn(
                    "glue", "catalog", ignore_prefix=True)]))

        # Glue read only listing all to be viewable in console
        read_only_policy.add_statements(iam.PolicyStatement(actions=[
            "glue:ListCrawlers",
            "glue:ListDevEndpoints",
            "glue:ListJobs",
            "glue:ListMLTransforms",
            "glue:ListRegistries",
            "glue:ListSchemas",
            "glue:ListSchemaVersions",
            "glue:ListTriggers",
            "glue:ListWorkflows",
            "glue:BatchGetDevEndpoints",
            "glue:BatchGetJobs",
            "glue:BatchGetPartition",
            "glue:BatchGetTriggers",
            "glue:GetCrawlerMetrics",
            "glue:BatchGetCrawlers"], resources=["*"]))

        # Cloudwatch logs access to glue and lambda resources
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["logs:Describe*", "logs:Get*", "logs:List*",
                         "logs:StartQuery", "logs:StopQuery",
                         "logs:TestMetricFilter", "logs:FilterLogEvents"],
                resources=[self.build_context.resource_arn(
                    "logs", "log-group:/aws/lambda/"),
                    self.build_context.resource_arn(
                    "logs", "log-group:/aws-glue/*",
                    ignore_prefix=True),
                    self.build_context.resource_arn(
                    "logs", "log-group::log-stream:*",
                    ignore_prefix=True)]))

        # Additional listing actions which requires access to all resources
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=[
                    "lakeformation:GetDataAccess", "s3:ListAllMyBuckets",
                    "apigateway:GET", "athena:List*"],
                resources=["*"]))

        # Limited read/write only permissions to buckets withc stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:GetObject",
                         "s3:GetAccountPublicAccessBlock",
                         "s3:GetBucketPublicAccessBlock",
                         "s3:GetBucketPolicyStatus", "s3:GetBucketAcl",
                         "s3:ListAccessPoints", "s3:PutObject",
                         "s3:GetBucketLocation",
                         "s3:GetObjectVersion",
                         "s3:AbortMultipartUpload",
                         "s3:CreateMultipartUpload",
                         "s3:PutObjectAcl"
                         ],
                resources=[self.build_context.regionless_resource_arn(
                    "s3", self.build_context.get_stack_prefix()),
                    self.build_context.regionless_resource_arn(
                    "s3", self.build_context.get_asset_bucket()),
                    self.build_context.regionless_resource_arn(
                    "s3", self.build_context.get_asset_bucket(
                        truncated=True),
                    not_all=True)]))

        # Allows athena queries and table access for workgroup created by stack
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["athena:StartQueryExecution", "athena:Batch*", "athena:Get*"],
                resources=[self.build_context.resource_arn("athena", "workgroup/")]))

        # Get and list permissions for lambda functions with the stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["lambda:Get*", "lambda:List*", "lambda:InvokeFunction"],
                resources=[self.build_context.resource_arn(
                    "lambda", "function:")]))

        # Lambda permissions required for console list view
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["lambda:GetAccountSettings", "lambda:ListFunctions",
                         "lambda:ListTags"],
                resources=["*"]))

        # Read permissions to see details of step functions with stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["states:DescribeStateMachine",
                         "states:ListExecutions",
                         "states:ListTagsForResource",
                         "states:StartExecution"],
                resources=[self.build_context.resource_arn(
                    "states", "stateMachine:")]))

        # SNS permissions to see a list of all step functions in console
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["states:ListStateMachines"],
                resources=[self.build_context.resource_arn(
                    "states", "stateMachine:*",
                    ignore_prefix=True)]))

        # Read permissions to see details of step function executions with stack prefix
        read_only_policy.add_statements(
            iam.PolicyStatement(
                actions=["states:Describe*", "states:Get*"],
                resources=[self.build_context.resource_arn(
                    "states", "execution:")]))

        # Attach policies to role
        group_assume_policy.attach_to_group(group)
        read_only_policy.attach_to_role(read_role)
        return (group, read_role)
