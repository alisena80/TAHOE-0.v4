{% if 'download' == pipeline %}
from stacks.datasources.shared.interim_step_function import InterimStepFunction
from stacks.datasources.shared.retrieval_step_function import RetrievalStepFunction
{% endif %}
{% if 'query' == pipeline %}
from stacks.datasources.shared.query_retrieval_step_function import QueryRetrievalStepFunction
{% endif %}
{% if 'incremental' == pipeline %}
from stacks.datasources.shared.hudi_incremental_step_function import HudiIncremetalStepFunction
{% endif %}
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack


class {{name}}DatasourceInfrastructureStack(TahoeDatasourceNestedStack):
    def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix, build_context, base_context, log) -> None:
        super().__init__(scope, construct_id, build_context,
                         data_prefix, base_context, log)
        self.logger.info("Compiling {{name}} datasource")
    {% if 'download' == pipeline %}
        RetrievalStepFunction(self, "download", self.build_context, self.data_prefix, self.base_context, self.sns_config, self.sub_path,
                              log=self.log).build_stepfunction()            
        InterimStepFunction(self, "interim", self.build_context, self.data_prefix, self.base_context, self.sns_config, self.etl_databases_config, self.sub_path,
                            log=self.log, validate={{validate}}, preprocess={{preprocess}}).build_stepfunction()
    {%- endif %}
    {% if 'incremental' == pipeline %}
        HudiIncremetalStepFunction(self, "interim", self.build_context, self.data_prefix, self.base_context, self.sns_config, self.etl_databases_config, self.sub_path,
                                   log=self.log, validate={{validate}}).build_stepfunction()
    {%- endif %}
    {% if 'query' == pipeline %}
        QueryRetrievalStepFunction(self, "query", self.build_context, self.data_prefix, self.base_context, self.etl_databases_config, self.sub_path,
                                   log=self.log).build_stepfunction()
    {%- endif %}

                           