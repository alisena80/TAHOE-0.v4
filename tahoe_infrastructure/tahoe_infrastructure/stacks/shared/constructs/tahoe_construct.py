from constructs import Construct
from stacks.shared.lib.build_config import BuildConfig
import inspect
import logging


class TahoeConstruct(Construct):
    """

    Create tahoe construct.

    build_context: BuildConfig
        The build config for the deployment
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig):
        """

        Create tahoe construct.

        Parameters
        ----------
            scope: Construct
                datasource to check log status
            id: str
                Logical id
            build_context: BuildConfig
                Build context

        """
        super().__init__(scope, id)
        self.build_context: BuildConfig = build_context
        self.create_logger()

    def create_logger(self):
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        self.logger = logging.getLogger(mod.__name__)

    def can_deploy(self, datasource: str):
        """

        Check config to see if datasource needs to be deployed.

        datasource: str
            datasource check deploy status
        """
        return self.build_context.get_datasource_config(datasource)["deploy"]

    def can_log(self, datasource: str):
        """

        Check config to see if datasource has logging.

        Parameters
        ----------
            datasource: str
                datasource to check log status
        """
        return self.build_context.get_datasource_config(datasource)["log"]

    def can_sync(self):
        """

        Check config to see if datasource has logging.

        Parameters
        ----------
            datasource: str
                datasource to check log status
        """
        return self.build_context.get_optional("sync_cdk_to_etl_parameters")
