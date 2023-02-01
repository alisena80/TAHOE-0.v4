from constructs import Construct
from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.consumers.shared.stored_consumer_step_function import StoredConsumerStepFunction
from stacks.shared.constructs.etl_job import Etl


class CyhyRedshiftStepFunction(StoredConsumerStepFunction):

    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path,
                 datasources: DependentDatasources):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path, datasources)
        extra_python_files = [self.tahoe_commons]

        copy_to_redshift = Etl(
            self, "copyToRedshift", build_context, data_prefix, base_context, "copy_to_redshift", self.sub_path,
            python_files=extra_python_files,
            DATABASE=self.datasources.get_datasource("cyhy").etl_databases_config.interim_database.database_input.name,
            STACK_PREFIX=self.build_context.get_stack_prefix(),
            SCHEMA=self.build_context.redshift_schema("cyhy"),
            ROLE=self.base_context.get_pipeline_role().role_arn,
            TEMPDIR=self.base_context.buckets.raw_bucket.s3_url_for_object("temp"),
            connections=["redshift_connection"])

        sql_query = f'create schema if not exists {self.build_context.redshift_schema("cyhy")};'
        create_schema = self.base_context.functions.run_redshift_command_task(
            self, "create_redshift_schema", sql_query) .next(copy_to_redshift.definition)

        self.set_custom_definition(create_schema)

