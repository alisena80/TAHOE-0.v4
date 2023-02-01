# IMPORTS START
from awsglue.transforms import *
from tahoecommon.common_pydeequ.nist_mitre_data_tests import NistMitreDataTests
from awsglue.dynamicframe import DynamicFrameCollection

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
nist_mitre_data_tests = NistMitreDataTests(s3_output)
spark = nist_mitre_data_tests.pydeequ_helper.get_spark()
glueContext = GlueContext(nist_mitre_data_tests.get_spark_context())
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# SPARK END

# LOG START
logger = log.TahoeLogger().getSparkLogger(glueContext.get_logger())
# LOG END


# CODE START

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=stack_prefix + "mitre",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("cwe-id", "long", "cwe-id", "long"),
        ("name", "string", "name", "string"),
        ("weakness abstraction", "string", "weakness abstraction", "string"),
        ("status", "string", "status", "string"),
        ("description", "string", "description", "string"),
        ("extended description", "string", "extended description", "string"),
        ("related weaknesses", "string", "related weaknesses", "string"),
        ("weakness ordinalities", "string", "weakness ordinalities", "string"),
        ("applicable platforms", "string", "applicable platforms", "string"),
        ("background details", "string", "background details", "string"),
        ("alternate terms", "string", "alternate terms", "string"),
        ("modes of introduction", "string", "modes of introduction", "string"),
        ("exploitation factors", "string", "exploitation factors", "string"),
        ("likelihood of exploit", "string", "likelihood of exploit", "string"),
        ("common consequences", "string", "common consequences", "string"),
        ("detection methods", "string", "detection methods", "string"),
        ("potential mitigations", "string", "potential mitigations", "string"),
        ("observed examples", "string", "observed examples", "string"),
        ("functional areas", "string", "functional areas", "string"),
        ("affected resources", "string", "affected resources", "string"),
        ("taxonomy mappings", "string", "taxonomy mappings", "string"),
        ("related attack patterns", "string", "related attack patterns", "string"),
        ("notes", "string", "notes", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

tables = DynamicFrameCollection({"nist_mitre": ApplyMapping_node2}, glueContext)
nist_mitre_data_tests.generate_statistics(tables)
# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": s3_output, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)
logger.dblog(log.StatusLog("nist_mitre", log.Status.RELATIONALIZED))

job.commit()

# CODE END
