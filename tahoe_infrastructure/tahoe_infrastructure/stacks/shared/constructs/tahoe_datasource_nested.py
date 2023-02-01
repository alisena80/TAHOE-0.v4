from venv import create
from aws_cdk import NestedStack
from stacks.shared.constructs.sns_config import SnsConfig
from stacks.shared.constructs.etl_database_config import EtlDatabaseConfig

from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig
import os
import logging
import inspect

class TahoeDatasourceNestedStack(NestedStack):
    """
    A class to represent a tahoe datasource nested stack. Creates basic requirements for every new data pipeline

    Attributes
    ----------
    build_context  : str
        name of the state machine
    base_context  : BaseConfig
        step function steps default configuration for etl job
    data_prefix: str
        data prefix to name/describe
    sns_config: SnsConfig
        Creats sns configuration to pipeline data steps
    etl_databases_config: EtlDatabaseConfig
        Glue databases and permissions for the datasource
    sub_path: str
        Absolute path to datasource
    log: bool
        Log datasource

    """
    def __init__(self, scope: TahoeConstruct, construct_id: str, build_context: BuildConfig, data_prefix: str, base_context: BaseConfig, log: bool) -> None:
        """
        Creates nested datasource

        Parameters
        ----------
            scope : Construct
                construct scope
            construct_id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            data_prefix: str
                data prefix to name/describe
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            log: bool
                Log datasource
        
        """
        super().__init__(scope, construct_id)
        self.build_context: BuildConfig = build_context
        self.base_context: BaseConfig = base_context
        self.data_prefix: str = data_prefix
        os.chdir("../datasources")
        self.sub_path = os.getcwd()
        self.sns_config = SnsConfig(
            self, "snsConfiguration", self.build_context, self.data_prefix, self.base_context)
        self.etl_databases_config = EtlDatabaseConfig(
            self, "etlDatabases", self.build_context, self.data_prefix, self.base_context)
        self.log = log
        self.is_queryable = False
        self.create_logger()

    def create_logger(self):
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        self.logger = logging.getLogger(mod.__name__)
        
    def queryable(self):
        self.is_queryable = True

