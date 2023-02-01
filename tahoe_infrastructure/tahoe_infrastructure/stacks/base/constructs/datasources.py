from constructs import Construct
from stacks.datasources.cdm.cdm_datasource_stack import CdmDatasourceInfrastructureStack
from stacks.datasources.mitre.mitre_datasource_stack import MitreDatasourceInfrastructureStack
from stacks.datasources.nvd.nvd_datasource_stack import NvdDatasourceInfrastructureStack
from stacks.datasources.cyhy.cyhy_datasource_stack import CyhyDatasourceInfrastructureStack
from stacks.datasources.shodan.shodan_datasource_stack import ShodanDatasourceInfrastructureStack

from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig


class Datasources(TahoeConstruct):
    """
    A class to represent an  encapsulation of all the datasources.


    Attributes
    ----------
    mitre_datasource  : TahoeDatasourceNestedStack
        mitre datasource nested stack pipeline
    nvd_datasource  : TahoeDatasourceNestedStack
        nvd datasource nested stack pipeline
    cyhy_datasource  : TahoeDatasourceNestedStack
        cyhy datasource nested stack pipeline
    cdm_datasource : TahoeDatasourceNestedStack
        cdm datasource nestedd stack pipeline
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, base_context: BaseConfig):
        """
        Constructs all nested datasources

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
        """
        super().__init__(scope, id, build_context)

        self.nist_mitre_datasource = None
        if self.can_deploy("nist_mitre"):
            self.nist_mitre_datasource = MitreDatasourceInfrastructureStack(
                self, "nist_mitre", data_prefix="nist_mitre", build_context=self.build_context,
                base_context=base_context, log=self.can_log("nist_mitre"))

        self.nvd_datasource = None
        if self.can_deploy("nvd"):
            self.nvd_datasource = NvdDatasourceInfrastructureStack(
                self, "nvd", data_prefix="nvd", build_context=self.build_context, base_context=base_context,
                log=self.can_log("nvd"))

        self.cyhy_datasource = None
        if self.can_deploy("cyhy"):
            self.cyhy_datasource = CyhyDatasourceInfrastructureStack(
                self, "cyhy", data_prefix="cyhy", build_context=self.build_context, base_context=base_context,
                log=self.can_log("cyhy"))

        self.cdm_datasource = None
        if self.can_deploy("cdm"):
            self.cdm_datasource = CdmDatasourceInfrastructureStack(
                self, "cdm", data_prefix="cdm", build_context=self.build_context, base_context=base_context,
                log=self.can_log("cdm"))

        self.shodan_datasource = None
        if self.can_deploy("shodan"):
            self.shodan_datasource = ShodanDatasourceInfrastructureStack(
                self, "shodan", data_prefix="shodan", build_context=self.build_context, base_context=base_context,
                log=self.can_log("shodan"))
        



