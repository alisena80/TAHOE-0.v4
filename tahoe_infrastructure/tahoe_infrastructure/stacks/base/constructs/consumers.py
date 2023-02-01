from constructs import Construct
from stacks.base.constructs.datasources import Datasources
from stacks.consumers.cdm_test_consumer.cdm_test_consumer_stack import CdmTestConsumer
from stacks.consumers.cyhy_redshift.cyhy_redshift_consumer_stack import CyhyRedshiftConsumer
from stacks.consumers.docex_s3.docex_s3_consumer_stack import DocExS3Consumer

from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig
from stacks.shared.lib.dependent_datasources import DependentDatasources


class Consumers(TahoeConstruct):
    """
    A class to represent an encapsulation of all the consumers.


    Attributes
    ----------
    docex_s3  : NestedStack
        Docex s3 consumer nested stack
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, base_context: BaseConfig,
                 datasources: Datasources):
        """
        Constructs all nested consumers

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            datasources : Datasources
                All datasources created in base stack
        """
        super().__init__(scope, id, build_context)

        docex_s3_dependents = DependentDatasources(datasources.nist_mitre_datasource, datasources.nvd_datasource)
        if docex_s3_dependents.deployable():
            self.docex_s3 = DocExS3Consumer(self, "docexs3", data_prefix="docexs3", build_context=build_context,
                                            base_context=base_context, datasources=docex_s3_dependents)

        cyhy_redshift_dependents = DependentDatasources(datasources.cyhy_datasource)
        if cyhy_redshift_dependents.deployable():
            self.cyhy_redshift = CyhyRedshiftConsumer(self, "cyhyRedshift", data_prefix="cyhyredshift",
                                                      build_context=build_context, base_context=base_context,
                                                      datasources=cyhy_redshift_dependents)






