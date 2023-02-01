
from pandas import DataFrame
from pydeequ.analyzers import Completeness, Size
from tahoecommon.common_pydeequ.pydeequ_helper import PydeequHelper
from datetime import datetime

class DataTests:
    def __init__(self, output_location: str, spark=None):
        # Create an output path for the json in the format of "bucket/stats/data_source/date/stats.json"
        # The data_source is already appended to output_location
        location = output_location.split("/")
        location.insert(len(location) - 1, "stats")
        self.write_path: str = "/".join(location) + "/" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "/stats.json"
        self.pydeequ_helper:PydeequHelper = PydeequHelper(self.write_path, spark)

    def get_spark_context(self):
        """
        Shortcut to PydeequHelper's spark object
        :return:
        """
        return self.pydeequ_helper.get_spark_context()

    def generate_statistics(self, tables) -> DataFrame:
        '''
        Generates basic statistcs for data. 

        :param tables: Dynamic frame collection that creates stats on file
        '''
        for df_name in tables.keys():
            print("TABLE:", df_name)
            m_df = tables.select(df_name).toDF()
            for x in m_df.columns:
                m_df = m_df.withColumnRenamed(x, x.replace(".", "_"))

            analysis_runner = self.pydeequ_helper.create_analysis_runner(m_df)
            analysis_runner = analysis_runner.addAnalyzer(Size())

            for x in m_df.columns:
                analysis_runner = analysis_runner.addAnalyzer(Completeness(x))

            analysis_result = self.pydeequ_helper.run_analysis(analysis_runner, df_name)

    def check_constraints(self, name: str, frame_relationalized: DataFrame) -> DataFrame:
        pass