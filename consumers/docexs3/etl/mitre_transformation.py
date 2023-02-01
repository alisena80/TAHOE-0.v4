# IMPORTS START
from awsglue.transforms import *
from pyspark.sql.dataframe import *
from awsglue.dynamicframe import DynamicFrame

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME", "S3_OUTPUT", "STACK_PREFIX"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
s3_output = args["S3_OUTPUT"]
stack_prefix = args["STACK_PREFIX"]
# ARG END


# SPARK START
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# SPARK END

# LOG START
logger = log.TahoeLogger().getSparkLogger(glueContext.get_logger())
# LOG END


# CODE START
mitre_dynamic = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=stack_prefix + "mitre",
    transformation_ctx="S3bucket_node1",
)


mitre_dataframe: DataFrame = mitre_dynamic.toDF()

# drop duplicates
mitre_dataframe.drop_duplicates()

mitre_dataframe = DynamicFrame.fromDF(
    mitre_dataframe, glueContext, "mitre_dataframe")

# Script generated for node S3 bucket
output = glueContext.write_dynamic_frame.from_options(
    frame=mitre_dataframe,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": s3_output},
    format_options={"compression": "snappy"},
    transformation_ctx="output"
)

# CODE END
