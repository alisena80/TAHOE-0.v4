from constructs import Construct
import aws_cdk.aws_stepfunctions as sf

from constructs import Construct
from stacks.shared.lib.dependent_datasources import DependentDatasources
from stacks.consumers.shared.stored_consumer_step_function import StoredConsumerStepFunction
from stacks.shared.constructs.etl_job import Etl


class DocExS3StepFunction(StoredConsumerStepFunction):

    def __init__(self, scope: Construct, id: str, build_context, base_context, data_prefix, sub_path,
                 datasources: DependentDatasources):
        super().__init__(scope, id, build_context, base_context, data_prefix, sub_path, datasources)
        
        mitre_dedupe_location = self.build_context.s3_url(self.base_context.get_interim_bucket(), "nist_mitre_dedupe")

        nvd_vendor_location = self.build_context.s3_url(self.base_context.get_interim_bucket(), "nvd_vendor")

        mitre_dedup = Etl(
            self, "mitreDedupe", build_context, data_prefix, base_context, "mitre_transformation", self.sub_path,
            S3_OUTPUT=mitre_dedupe_location,
            DATABASE=datasources.get_datasource("nist_mitre").etl_databases_config.interim_database.database_input.name,
            STACK_PREFIX=self.build_context.get_stack_prefix())

        vendor_dataset = Etl(
            self, "vendorDataset", build_context, data_prefix, base_context, "nvd_transformation", self.sub_path,
            S3_OUTPUT=nvd_vendor_location,
            DATABASE=datasources.get_datasource("nvd").etl_databases_config.interim_database.database_input.name,
            STACK_PREFIX=self.build_context.get_stack_prefix())

        combined_dataset = Etl(
            self, "final", build_context, data_prefix, base_context, "combine_transformation", self.sub_path,
            OUTPUT_BUCKET=self.base_context.get_curated_bucket().bucket_name,
            DATABASE=datasources.get_datasource("nvd").etl_databases_config.interim_database.database_input.name,
            MITRE_DEDUPE_OUTPUT=mitre_dedupe_location, NVD_VENDOR_OUTPUT=nvd_vendor_location,
            STACK_PREFIX=self.build_context.get_stack_prefix())
        check_result = sf.Choice(self, "sourceTrigger")
        
        self.set_custom_definition(check_result.when(sf.Condition.string_equals("$.input.Source", "nist_mitre"),
                                                     mitre_dedup.definition.next(combined_dataset.definition))
                                   .when(sf.Condition.string_equals("$.input.Source", "nvd"),
                                         vendor_dataset.definition.next(combined_dataset.definition)))
