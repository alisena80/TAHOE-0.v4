from aws_cdk import NestedStack
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig
import os
import inspect
import logging

class TahoeConsumerNestedStack(NestedStack):
    """
    A class to represent a tahoe consumer nested stack. Creates basic requirements for every new data pipeline.

    Attributes
    ----------
    build_context : BuildConfig
        build configuration settings
    base_context: BaseConfig
        Contains all base stack info that needs to be exposed
    data_prefix: str
        data prefix to name/describe
    sub_path: str
        Absolute path to consumer

    """
    def __init__(self, scope: TahoeConstruct, construct_id: str, build_context, base_context, data_prefix) -> None:
        """
        A class to represent a tahoe consumer nested stack. Creates basic requirements for every new data pipeline.

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
            data_prefix: str
                data prefix to name/describe
            sub_path: str
                Absolute path to consumer

        """
        super().__init__(scope, construct_id)
        self.build_context: BuildConfig = build_context
        self.base_context: BaseConfig = base_context
        self.data_prefix = data_prefix
        os.chdir("../consumers")
        self.sub_path = os.getcwd()
        self.create_logger()
    
    def create_logger(self):
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        self.logger = logging.getLogger(mod.__name__)