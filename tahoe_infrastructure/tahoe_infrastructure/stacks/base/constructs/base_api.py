import os
import zipfile

import shutil

from aws_cdk import Duration
from constructs import Construct
import aws_cdk.aws_apigateway as api
import aws_cdk.aws_lambda as l
from stacks.shared.constructs.tahoe_construct import TahoeConstruct


class BaseApi(TahoeConstruct):
    '''
    Base api configuration deploys lambda and api gateway

    Arguments
    ----------
        api_router: l.Function
            Lambda function router
        tahoe_api: api.LambdaRestApi
            Api configuration
    '''
    def __init__(self, scope: Construct, id: str, build_context, base_role):
        '''
        Constructs api

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            base_role : iam.Role
                Base role to run api with
        '''
        super().__init__(scope, id, build_context)

        # Copies the lambda py file into the base deployment package
        self.create_deployment_zip("../base/api/", "base-api-dependencies.zip", "packaged-api-lambda")

        self.api_router = l.Function(self, "apiRouter",
                                     runtime=l.Runtime.PYTHON_3_9,
                                     handler="api-endpoints.handler",
                                     code=l.Code.from_asset(
                                         "../base/api/packaged-api-lambda.zip"),
                                     role=base_role, timeout=Duration.seconds(600),
                                     layers=[self.build_context.get_logging_layer()])
        self.add_env("STACK_PREFIX", self.build_context.get_stack_prefix())
        self.add_env("NVD_SCHEMA", self.build_context.redshift_schema("nvd"))
        self.add_env("CYHY_SCHEMA", self.build_context.redshift_schema("cyhy"))

        if "gov" in self.build_context.get_region():
            endpoint = api.EndpointType.REGIONAL
        else:
            endpoint = api.EndpointType.EDGE
        self.tahoe_api = api.LambdaRestApi(self, "api",
                                           rest_api_name=self.build_context.api_name(),
                                           handler=self.api_router,
                                           cloud_watch_role=False,
                                           endpoint_types=[endpoint],
                                           default_method_options=api.MethodOptions(api_key_required=True))

        api_key = self.tahoe_api.add_api_key("apiKey",
                                             api_key_name="tahoe-key-" + self.build_context.get_stack_prefix(),
                                             description="Temporary API key to block api access")

        self.tahoe_api.add_usage_plan("usagePlan",
                                      api_stages=[api.UsagePlanPerApiStage(
                                          api=self.tahoe_api, stage=self.tahoe_api.deployment_stage)],
                                      throttle=api.ThrottleSettings(burst_limit=10, rate_limit=10)) \
            .add_api_key(api_key=api_key)

    def add_env(self, key: str, val: str):
        """
        Adds env variable to api lambda function

        key: str
            Key for lambda env variable
        val: str
            Value for lambda env variable
        """
        self.api_router.add_environment(key, val)

    def create_deployment_zip(self, working_dir, base_zip_name, output_zip_name):
        extracted_folder = os.path.join(working_dir, "extracted")
        base_zip_file = os.path.join(working_dir, base_zip_name)
        output_zip_file = os.path.join(working_dir, output_zip_name + ".zip")

        # Remove any existing extracted folders or built deployment zips
        if os.path.exists(extracted_folder):
            shutil.rmtree(extracted_folder)
        if os.path.exists(output_zip_file):
            os.remove(output_zip_file)

        # Extract the base deployment zip
        with zipfile.ZipFile(base_zip_file, "r") as file:
            os.mkdir(extracted_folder)
            file.extractall(extracted_folder)

        for file in os.listdir(working_dir):
            if file.endswith(".py"):
                shutil.copy(os.path.join(working_dir, file), extracted_folder)

        # Rezip everything. The extension will be added by the command, so leave it out of the name
        shutil.make_archive(os.path.join(working_dir, output_zip_name), "zip", extracted_folder)

        # Clean up
        shutil.rmtree(extracted_folder)
