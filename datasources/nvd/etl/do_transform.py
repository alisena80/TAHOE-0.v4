# IMPORTS START
from awsglue.transforms import *
from tahoecommon.common_pydeequ.nvd_data_tests import NvdDataTests
from tahoecommon.commonglue import write_relationalized_frame_to_output

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME", "S3_OUTPUT", "S3_TEMP", "STACK_PREFIX"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
s3_output = args["S3_OUTPUT"]
s3_temp = args["S3_TEMP"]
stack_prefix = args["STACK_PREFIX"]
# ARG END


# SPARK START
nvd_data_tests = NvdDataTests(s3_output)
spark = nvd_data_tests.pydeequ_helper.get_spark()
glueContext = GlueContext(nvd_data_tests.get_spark_context())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# SPARK END

# LOG START
logger = log.TahoeLogger().getSparkLogger(glueContext.get_logger())
# LOG END


# CODE START
def write_frame_to_output(frame):
    for df_name in frame.keys():
        m_df = frame.select(df_name)
        print("Writing to Parquet File: ", df_name)

        glueContext.write_dynamic_frame.from_options(frame=m_df,
                                                     connection_type="s3",
                                                     connection_options={"path": s3_output + "/" + df_name},  
                                                     format_options={"compression": "snappy"},  
                                                     format="glueparquet")


nvd_df = glueContext.create_dynamic_frame.from_catalog(
    database=database, table_name=stack_prefix + "nvd")


print("Count Vuln: " + str(nvd_df.count()))


nvd_df_table_collection = nvd_df.relationalize("nvd", s3_temp + "/nvd/")
nvd_data_tests.generate_statistics(nvd_df_table_collection)

write_relationalized_frame_to_output(glueContext, s3_output, nvd_df_table_collection)
job.commit()

# CODE END
