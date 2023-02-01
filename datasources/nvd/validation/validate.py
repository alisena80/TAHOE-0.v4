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
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
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
def validate(table_names):
    failures = {}
    for table in table_names:
        # retrival of dataframe may change with different datasources
        nvd_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
        nvd_frame = nvd_frame.toDF()
        # Check if data frame schema changed compared to glue crawled database
        validator = BasicSchemaValidator(nvd_frame, database, table)
        validator.compare_schema(exclude_callback=exclude_obj_callback)
        if validator.differences is not None:
            failures[table] = str(validator)
            print(failures)
    if len(failures) != 0:
        raise ValueError(str(failures))

# Raw crawler fails to pick this field up but underlying data contains this field and can be read. Interim table also contains this field. Ignore the field in order to pass validation


def exclude_obj_callback(obj, path):
    return True if "versionStartExcluding" in path else False


# retrive all tables to validate
table_names = get_tables_names(database)
print(table_names)
validate(table_names)
logger.dblog(log.StatusLog("nvd", log.Status.VALIDATED))
job.commit()

# CODE END
