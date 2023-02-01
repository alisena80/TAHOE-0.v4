from pandas import DataFrame
from pydeequ.analyzers import Completeness, Size
from tahoecommon.common_pydeequ.pydeequ_helper import PydeequHelper
from tahoecommon.common_pydeequ.data_tests import DataTests

class NvdDataTests(DataTests):
    def __init__(self, output_location):
        super().__init__(output_location)

    def check_constraints(self, name: str, cyhy_frame_relationalized: DataFrame) -> DataFrame:
        '''
        Runs constraint checks. Introduction of some data rejection if data is outside of expected assert values (TODO).
        '''
        pass