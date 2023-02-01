from pydeequ.analyzers import Completeness, Size
from tahoecommon.common_pydeequ.pydeequ_helper import PydeequHelper
from tahoecommon.common_pydeequ.data_tests import DataTests


class NistMitreDataTests(DataTests):
    def __init__(self, output_location):
        super().__init__(output_location)

    def check_constraints(self, name, mitre_frame_relationalized):
        '''
        Runs constraint checks. Introduction of some data rejection if data is outside of expected assert values (TODO).
        '''
        pass