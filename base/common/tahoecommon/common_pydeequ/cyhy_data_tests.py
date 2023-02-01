from pandas import DataFrame
from pydeequ.analyzers import Completeness, Size
from tahoecommon.common_pydeequ.pydeequ_helper import PydeequHelper
from datetime import datetime
from tahoecommon.common_pydeequ.data_tests import DataTests

class CyhyDataTests(DataTests):
    def __init__(self, output_location):
        super().__init__(output_location)

    def check_constraints(self, name, cyhy_frame_relationalized) -> DataFrame:
        '''
        Runs constraint checks. Introduction of some data rejection if data is outside of expected assert values (TODO).
        '''
        m_df = cyhy_frame_relationalized.select(name).toDF()
        constraint_check = self.pydeequ_helper.create_check(name).isPositive("ip_int")
        result_df = self.pydeequ_helper.run_check(m_df, name, constraint_check)
        result_df.show(200, False)

        return result_df
