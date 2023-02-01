# IMPORTS START
from tahoecommon.commonvalidation import BasicSchemaValidator
from tahoecommon.commonglue import *

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME", "TABLE_NAME", "UPDATE_LOCATION"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
table_name = args["TABLE_NAME"]
update_location = args["UPDATE_LOCATION"]
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
def exclude_obj_callback(obj, path):
    return True if "_hoodie" in path else False


def validate(table_names):
    failures = {}
    for table in table_names:
        # retrival of dataframe may change with different datasources
        cyhy_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": update_location.split(","), "recurse": True},
            format="json",
        ).resolveChoice(specs=[('ip_int', 'cast:int')]).toDF()
        print(f"Validating {cyhy_frame.count()} records")
        if cyhy_frame.count() < 0:
            raise ValueError(f"Nothing remaining to be proccessed in {table_name}")
        validator = BasicSchemaValidator(cyhy_frame, database, table)
        validator.compare_schema(exclude_obj_callback)
        if validator.differences is not None:
            failures[table] = str(validator)
            print(failures)
    if len(failures) != 0:
        raise ValueError(str(failures))


logger.dblog(log.StatusLog(f"cyhy_{table_name}", log.Status.VALIDATED))
validate([table_name])
job.commit()

# CODE END
