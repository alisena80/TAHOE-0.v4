# IMPORTS START
from awsglue.transforms import *
from tahoecommon.common_pydeequ.cyhy_data_tests import CyhyDataTests
from awsglue import DynamicFrame

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATA_PREFIX", "JOB_NAME", "PREPROCESS_BUCKET", "S3_OUTPUT", "S3_TEMP", "STACK_PREFIX", "TABLE_NAME", "UPDATE_LOCATION"])

data_prefix = args["DATA_PREFIX"]
job_name = args["JOB_NAME"]
preprocess_bucket = args["PREPROCESS_BUCKET"]
s3_output = args["S3_OUTPUT"]
s3_temp = args["S3_TEMP"]
stack_prefix = args["STACK_PREFIX"]
table_name = args["TABLE_NAME"]
update_location = args["UPDATE_LOCATION"]
# ARG END


# SPARK START
cyhy_data_tests = CyhyDataTests(s3_output)
spark = cyhy_data_tests.pydeequ_helper.get_spark()
glueContext = GlueContext(cyhy_data_tests.get_spark_context())
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
        d_df = m_df.toDF()
        hoodieless_col = [f"`{x}`" for x in d_df.columns if '_hoodie' not in x]
        # remove hudi related columns if they exist
        if len(hoodieless_col) != len(d_df.columns):
            d_df = d_df.selectExpr(*hoodieless_col)

        print("Writing to Parquet File: ", df_name)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # overwrite location
        writer = d_df.write.mode("overwrite").format("parquet").option("compression", "snappy")
        writer.save(f"{s3_output}/{df_name}")


def load_and_relationalize(name: str):
    hudiDf = spark. \
        read. \
        format("hudi"). \
        load(name).alias("hudiDf")
    cyhy_frame = DynamicFrame.fromDF(hudiDf, glue_ctx=glueContext, name=table_name)
    print("Count {}: ".format(name) + str(cyhy_frame.count()))
    if cyhy_frame.count() > 0:
        cyhy_frame_table_collection = cyhy_frame.relationalize(f"{table_name}", f"{s3_temp}/{data_prefix}_{table_name}")

        # Create stats
        cyhy_data_tests.generate_statistics(cyhy_frame_table_collection)
        cyhy_data_tests.check_constraints(name, cyhy_frame_table_collection)
        write_frame_to_output(cyhy_frame_table_collection)


load_and_relationalize(f"s3://{preprocess_bucket}/{data_prefix}/{table_name}")
logger.dblog(log.StatusLog(f"{data_prefix}_{table_name}", log.Status.RELATIONALIZED))

# CODE END
