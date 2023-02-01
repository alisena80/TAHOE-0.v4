# IMPORTS START
from awsglue.transforms import *
from pyspark.sql.dataframe import *
from tahoecommon.commonglue import *

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME", "ROLE", "SCHEMA", "STACK_PREFIX", "TEMPDIR"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
role = args["ROLE"]
schema = args["SCHEMA"]
stack_prefix = args["STACK_PREFIX"]
tempdir = args["TEMPDIR"]
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
# Gather a list of all cyhy tables to copy
glue_tables = get_tables_names(job_name)

for glue_table in glue_tables:
    # Ignore the tables that store json
    if "json" in glue_table:
        continue

    # Read the glue catalog entry for the given table and create a data frame
    frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=glue_table,
        redshift_tmp_dir=tempdir,
        additional_options={"aws_iam_role": role}
    )

    # Setup redshift connection options
    # The details of the connection are already configured as the "redshift_connection" glue connector
    redshift_connection_options = {
        "dbtable": f"{schema}.{glue_table}",
        "database": "dev",
        "aws_iam_role": role
    }

    # Write the frame to redshift
    # The catalog connection must be passed to the ETL job tso be found correctly
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=frame,
        catalog_connection="redshift_connection",
        connection_options=redshift_connection_options,
        redshift_tmp_dir=tempdir
    )

# CODE END
