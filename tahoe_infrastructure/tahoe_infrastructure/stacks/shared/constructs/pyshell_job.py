import copy
from aws_cdk import RemovalPolicy
from constructs import Construct
from typing import List
import os

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_stepfunctions_tasks as sft
import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_logs as logs

from stacks.shared.lib.enumerations import GlueVersions
from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct
import subprocess


class Pyshell(TahoePipelineConstruct):
    """
    A class to represent a tahoe configured pyshell job

    Attributes
    ----------
    name  : str
        name of the state machine
    etl  : glue.CfnJob
        glue etl job
    definition  : Chain
        step function steps default configuration for etl job

    """

    def __init__(
            self, scope: Construct, id: str, build_context, data_prefix, base_context, etl_name, main_folder, *,
            sub_folder="", connections: List[glue.IConnection] = None, dpu: float = 0.0625, python_files: List[str] = [],
            python_modules: List[str] = [],
            jars: List[str] = [],
            **params):
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
            main_folder: str
                root folder to find etl file
            sub_folder: str
                subfolder to find pyshell code in
            python_files: str
                python files to set in etl path
            dpu: float
                Dpu to set to the download function
            python modules: str
                python modules that must be downlaoded at run time and/or egg files
            jars: str
                pyspark jars to execute pysepak based libraries
            params: {str: str} 
                paramters to pass to the etl job during invocation. Names mirriored in etl python file
        """
        super().__init__(scope, id, build_context, base_context, data_prefix, main_folder)
        job_id = id + "pyshellJobs"
        self.name = self.build_context.glue_name_function(self.data_prefix, etl_name, job_id)
        self.params = params
        python_files.extend([self.base_context.get_bootstrap_bucket().s3_url_for_object(
            self.build_context.get_tahoe_common()), self.base_context.get_bootstrap_bucket()
            .s3_url_for_object(self.build_context.get_tahoe_logger())])

        self.log_group = logs.LogGroup(
            self, id + "logGroup", log_group_name=f"/aws-glue/jobs/{self.name}", removal_policy=RemovalPolicy.DESTROY)

        self.base_context.get_streams().add_filter(id + "Filter", self.log_group, self)
        self.script_location = os.path.join(main_folder, self.data_prefix, sub_folder, etl_name + ".py")
        self.code_template()
        self.etl = glue.Job(
            self, job_id, max_capacity=dpu, role=self.base_context.get_pipeline_role(),
            continuous_logging=glue.ContinuousLoggingProps(enabled=True, log_group=self.log_group),
            connections=connections, job_name=self.name, executable=glue.JobExecutable.python_shell(
                glue_version=glue.GlueVersion.V1_0, python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(self.script_location),))

        args = self.parameterize(params, python_files, python_modules, jars,
                                 connections is not None, GlueVersions.PYSHELL)
        self.definition = sft.GlueStartJobRun(
            self, id + "etlRun", glue_job_name=self.name, arguments=sf.TaskInput.from_object(args),
            integration_pattern=sf.IntegrationPattern.RUN_JOB, result_path="$.input")

    def code_template(self):
        """
        Create empty code template of etl job
        """
        if self.can_sync():
            params = set(self.params.keys())
            params.add("continuous-log-logGroup")
            p = subprocess.Popen(["tahoe", "create-etl-file", "-l", self.script_location, "-a",
                                 ",".join(sorted(params)), "-t", "PYSHELL"], stdout=subprocess.PIPE)
            out, err = p.communicate()
            if err:
                raise err
