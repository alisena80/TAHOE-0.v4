from typing import List
from aws_cdk import Duration, RemovalPolicy
from constructs import Construct
import os
import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_s3_assets as asset
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_logs as logs
import subprocess
from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct
from stacks.shared.lib.enumerations import GlueVersions


class Etl(TahoePipelineConstruct):
    """
    A class to represent a tahoe configured etl job

    Attributes
    ----------
    name  : str
        name of the state machine
    etl  : glue.CfnJob
        glue etl job
    definition  : Chain
        step function steps default configuration for etl job

    """

    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context, etl_name, path, *,
                 concurrency=1, connections=None, sub_folder="etl", python_modules: List[str] = [],
                 python_files: List[str] = [], jars: List[str] = [], **params):
        """
        Constructs glue etl for tahoe context

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            data_prefix: str
                data prefix to name/describe
            build_context : BuildConfig
                build configuration settings
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            etl_name : str
                etl file name and description of task
            path: str
                root folder to find etl file
            python_modules: str
                python modules installed using pip references wheel house as pip repo. Must be comma deliminated
                packages. ie. awswrangler,requests
            python_files: str
                pass the s3 location of a zip of python files to be referenced in glue job. Must be comma deliminated'
                locations. ie. s3://assets/pydeeque.zip,s3://assets/pydeeque.zip
            jars: str
                pass jar locations to provide java libraries to spark. Must be comma deliminated locations.
                ie. s3://assets/pydeeque.jar,s3://assets/pydeeque.jar
            params: {str: str} 
                parameters to pass to the etl job during invocation. Names mirrored in etl python file
        """
        super().__init__(scope, id, build_context, base_context, data_prefix, path)
        job_id = id + "etlJobs"
        self.name = self.build_context.glue_name_function(self.data_prefix, etl_name, job_id)
        self.params = params
        self.log_group = logs.LogGroup(
            self, id + "logGroup", log_group_name=f"/aws-glue/jobs/{self.name}", retention=logs.RetentionDays.INFINITE,
            removal_policy=RemovalPolicy.DESTROY)
        if connections is not None:
            connections = list(map(lambda x: glue.Connection.from_connection_name(
                self, id + x, connection_name=x) if type(x) is str else x, connections))
        self.base_context.get_streams().add_filter(id + "Filter", self.log_group, self)

        self.script_location = os.path.join(path, self.data_prefix, sub_folder, etl_name + ".py")
        self.code_template()
        self.etl = glue.Job(
            self, job_id, job_name=self.name, role=self.base_context.get_pipeline_role(),
            worker_count=2,
            worker_type=glue.WorkerType.G_1_X,
            continuous_logging=glue.ContinuousLoggingProps(
                enabled=True, log_group=self.log_group, conversion_pattern="%m"),
            max_concurrent_runs=concurrency, connections=connections,
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V3_0, python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(self.script_location)))
        args = {}
        # always add tahoelogger
        python_files.append(self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_tahoe_logger()))
        # always add tahoecommon (Additional modules will need to be added for some functionality)
        python_files.append(self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_tahoe_common()))

        args = self.parameterize(params, python_files, python_modules, jars,
                                 connections is not None, GlueVersions.PYSPARK)

        self.definition = sft.GlueStartJobRun(
            self, id + "etlRun", glue_job_name=self.name, arguments=sf.TaskInput.from_object(args),
            integration_pattern=sf.IntegrationPattern.RUN_JOB, result_path="$.input").add_retry(
            errors=["RetryError"],
            max_attempts=3, interval=Duration.minutes(5))

    def code_template(self):
        """
        Create empty code template of etl job
        """
        if self.can_sync():
            self.logger.info(f"SYNCING {self.script_location}")
            params = set(self.params.keys())
            params.add("JOB_NAME")
            p = subprocess.Popen(["tahoe", "create-etl-file", "-l", self.script_location, "-a",
                                 ",".join(sorted(params)), "-t", "PYSPARK"], stdout=subprocess.PIPE)
            out, err = p.communicate()
            if err:
                raise err
