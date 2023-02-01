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
# If validation fails and user rejects then data is not sent to interim but is kept in raw bucket. Reject the data until admin intervenes. Have some required columns that must exist.


def validate(table_names):
    failures = {}
    for table in table_names:
        # retrival of dataframe may change wiht different datasources
        mitre_frame = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table)
        mitre_frame = mitre_frame.toDF()
        # Check if data frame schema changed compared to glue crawled database
        validator = BasicSchemaValidator(mitre_frame, database, table)
        validator.compare_schema()
        if validator.differences != None:
            failures[table] = str(validator)
            print(failures)
    if len(failures) != 0:
        raise ValueError(str(failures))


# retrive tables to validate
table_names = get_tables_names(database)
validate(table_names)
logger.dblog(log.StatusLog("nist_mitre", log.Status.VALIDATED))
job.commit()

# CODE END
