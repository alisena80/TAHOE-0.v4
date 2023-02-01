from stacks.datasources.shared.hudi_incremental_step_function import HudiIncremetalStepFunction
from stacks.datasources.shared.interim_step_function import InterimStepFunction
from stacks.datasources.shared.retrieval_step_function import RetrievalStepFunction
from stacks.shared.constructs.etl_job import Etl
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack


class CyhyDatasourceInfrastructureStack(TahoeDatasourceNestedStack):

    def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix, build_context, base_context, log) -> None:
        super().__init__(scope, construct_id, build_context,
                         data_prefix, base_context, log)
        self.logger.info("Compiling cyhy datasource")
        HudiIncremetalStepFunction(self, "interim", build_context, data_prefix,
                            base_context, self.sns_config, self.etl_databases_config, self.sub_path,
                            validate=True, log=self.log) \
                            .build_stepfunction()    
    
