from stacks.datasources.shared.interim_step_function import InterimStepFunction
from stacks.datasources.shared.retrieval_step_function import RetrievalStepFunction
from stacks.shared.constructs.pyshell_job import Pyshell
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig


class MitreDatasourceInfrastructureStack(TahoeDatasourceNestedStack):

    def __init__(self, scope: TahoeConstruct, construct_id: str, *, data_prefix: str, build_context: BuildConfig, base_context: BaseConfig, log: bool) -> None:
        super().__init__(scope, construct_id, build_context,
                         data_prefix, base_context, log)

        self.logger.info("Compiling nist_mitre datasource")
        InterimStepFunction(self, "interim", self.build_context, self.data_prefix, self.base_context,
                            self.sns_config, self.etl_databases_config, self.sub_path,
                            log=self.log, validate=True) \
                            .build_stepfunction()

        RetrievalStepFunction(self, "retrieval", build_context, data_prefix, base_context, self.sns_config, self.sub_path,
                              url='https://cwe.mitre.org/data/csv/',
                              log=self.log) \
                              .build_stepfunction()
