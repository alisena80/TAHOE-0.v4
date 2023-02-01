import pydeequ
from pydeequ import Check, CheckLevel, AnalysisRunner
from pydeequ.analyzers import AnalyzerContext
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import SparkSession


class PydeequHelper:

    def __init__(self, write_path, spark=None):
        if spark:
            self.spark = spark.config("spark.jars.packages", pydeequ.deequ_maven_coord).config(
                "spark.jars.excludes", pydeequ.f2j_maven_coord).getOrCreate()
        else:
            self.spark = SparkSession.builder.config("spark.jars.packages", pydeequ.deequ_maven_coord).config(
                "spark.jars.excludes", pydeequ.f2j_maven_coord).getOrCreate()
        self.metrics_repository = FileSystemMetricsRepository(self.spark, write_path)

    def create_check(self, name):
        """
        Creates a pydeequ Check object which queries can be added onto.
        :param name: Name to give this Check object
        :return: Check object
        """
        return Check(self.spark, CheckLevel.Warning, name)

    def run_check(self, df, name, check):
        """
        Runs the given Check query against the data
        :param df: Data to check
        :param name: Name of the table
        :param check: Check object with query info, created with create_check()
        :return: Data frame of verified results
        """
        key_tags = {"table": name}
        result_key = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        if type(check) != list():
            check = [check]
        check_result = VerificationSuite(self.spark) \
                        .onData(df)
        for x in check: 
            check_result.addCheck(x)

        check_result = check_result \
        .useRepository(self.metrics_repository) \
        .saveOrAppendResult(result_key) \
        .run()

        return VerificationResult.checkResultsAsDataFrame(self.spark, check_result)

    def create_analysis_runner(self, df):
        """
        Creates a pydeequ AnalysisRunner using the given data. Analyzers can be added
        to the resulting object to build the analysis
        :param df: Data to analyze
        :return: AnalysisRunner object
        """
        return AnalysisRunner(self.spark).onData(df)

    def run_analysis(self, analysis_runner, name):
        """
        Uses the given analysis runner to scan the data
        :param analysis_runner: AnalysisRunner object made with create_analysis_runner(). Has the data already.
        :param name: Name of the table
        :return: Data frame of analyzed metrics
        """
        key_tags = {"table": name}
        result_key = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        analysis_result = analysis_runner.useRepository(self.metrics_repository).saveOrAppendResult(result_key).run()
        return AnalyzerContext.successMetricsAsDataFrame(self.spark, analysis_result)

    def get_spark_context(self):
        return self.spark.sparkContext
    
    def get_spark(self):
        return self.spark
