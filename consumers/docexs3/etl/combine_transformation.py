# IMPORTS START
from pyspark.sql.dataframe import DataFrame
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import collect_set, concat, concat_ws, lit, split, date_format, initcap, col, regexp_replace
import boto3
from pyspark.sql.functions import substring_index
from pyspark.sql.types import DateType, IntegerType

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATABASE", "JOB_NAME", "MITRE_DEDUPE_OUTPUT", "NVD_VENDOR_OUTPUT", "OUTPUT_BUCKET", "STACK_PREFIX"])

database = args["DATABASE"]
job_name = args["JOB_NAME"]
mitre_dedupe_output = args["MITRE_DEDUPE_OUTPUT"]
nvd_vendor_output = args["NVD_VENDOR_OUTPUT"]
output_bucket = args["OUTPUT_BUCKET"]
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
s3 = boto3.resource("s3")
bucket: boto3 = s3.Bucket(output_bucket)
#--------------------------------------------------#

def get_nvd_data_catalog(table):
    # Script generated for node S3 bucket
    return glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx=table,
    )


# get dynamic frame of mitre deduped data
mitre_deduped: DataFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [mitre_dedupe_output], "recurse": True},
    format="parquet"
).toDF()

mitre_deduped.printSchema()

# get dynamic frame of nvd vendor data
nvd_vendor: DataFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [nvd_vendor_output], "recurse": True},
    format="parquet"

).toDF()

nvd_vendor.printSchema()
# get all base nvd data required to join mitre and nvd data
nvd_cve_items: DataFrame = get_nvd_data_catalog(
    stack_prefix + "nvd").toDF()
nvd_description: DataFrame = get_nvd_data_catalog(
    stack_prefix + "nvd_cve_description_description_data").toDF()
nvd_mitre: DataFrame = get_nvd_data_catalog(
    stack_prefix + "nvd_cve_problemtype_problemtype_data_val_description").toDF()

# Combine nvd and mitre data to get cwes
nvd_mitre_joined = nvd_mitre.select(nvd_mitre["id"].alias("nvdId"),
                                    nvd_mitre["index"].alias("nvdIndex"),
                                    substring_index(
                                        nvd_mitre["`cve.problemtype.problemtype_data.val.description.val.value`"],
                                        "-", -1).cast(IntegerType()).alias("cwe-id")).join(mitre_deduped, on="cwe-id",
                                                                                           how="inner")
nvd_mitre_joined = nvd_mitre_joined.select(
    "nvdId", concat("cwe-id", lit("-"),
                    "name").alias("cwe_plus_name")).groupBy("nvdId").agg(
    collect_set("cwe_plus_name").alias("cwe_plus_name")).select(
    "nvdId", concat_ws(",", "cwe_plus_name").alias("cwes"))
nvd_mitre_joined.show()
nvd_mitre_joined.printSchema()
# join cwe and vendor info to all items
nvd_cve_items = nvd_cve_items.join(
    nvd_description, nvd_cve_items['`cve.description.description_data`'] == nvd_description["id"],
    how="left").join(
    nvd_mitre_joined, on=nvd_mitre_joined["nvdId"] == nvd_cve_items["`cve.problemtype.problemtype_data`"],
    how="left").join(
    nvd_vendor, on=nvd_vendor["mainId"] == nvd_cve_items["`configurations.nodes`"],
    how="left")

dates = split(nvd_cve_items["`publisheddate`"], "T")
nvd_cve_items = nvd_cve_items.select(
    "*", date_format(dates.getItem(0), "dd MMM yyyy").alias("publisheddate"))
nvd_cve_items.printSchema()
nvd_cve_items.show()

nvd_cve_items.withColumn(
    "cve.description.description_data.val.value",
    regexp_replace(col("`cve.description.description_data.val.value`"),
                   "[\n\r]", " ")).withColumn(
    "cwes", regexp_replace(col("`cwes`"),
                           "[\n\r]", " "))


docex = DynamicFrame.fromDF(nvd_cve_items, glueContext, "docex")


ApplyMapping_node2 = ApplyMapping.apply(
    frame=docex,
    mappings=[
        ("`cve.cve_data_meta.id`", "string", "`meta.id`", "string"),
        ("`cve.description.description_data.val.value`",
         "string", "description_data", "string"),
        ("publisheddate", "string", "publisheddate", "string"),
        ("`impact.basemetricv2.acinsufinfo`",
         "boolean", "basemetricv2_acinsufinfo", "string"),
        ("`impact.basemetricv2.cvssv2.accesscomplexity`",
         "string", "basemetricv2_accesscomplexity", "string"),
        ("`impact.basemetricv2.cvssv2.accessvector`", "string",
         "`impact.basemetricv2.cvssv2.accessvector`", "string"),
        ("`impact.basemetricv2.cvssv2.authentication`", "string",
         "`impact.basemetricv2.cvssv2.authentication`", "string"),
        ("`impact.basemetricv2.cvssv2.availabilityimpact`", "string",
         "`impact.basemetricv2.cvssv2.availabilityimpact`", "string"),
        ("`impact.basemetricv2.cvssv2.basescore`", "double",
         "`impact.basemetricv2.cvssv2.basescore`", "string"),
        ("`impact.basemetricv2.cvssv2.confidentialityimpact`", "string",
         "`impact.basemetricv2.cvssv2.confidentialityimpact`", "string"),
        ("`impact.basemetricv2.exploitabilityscore`", "double",
         "`impact.basemetricv2.exploitabilityscore`", "string"),
        ("`impact.basemetricv2.impactscore`", "double",
         "`impact.basemetricv2.impactscore`", "string"),
        ("`impact.basemetricv2.cvssv2.integrityimpact`", "string",
         "`impact.basemetricv2.cvssv2.integrityimpact`", "string"),
        ("`impact.basemetricv2.obtainallprivilege`",
         "boolean", "obtainallprivilege", "string"),
        ("`impact.basemetricv2.obtainotherprivilege`",
         "boolean", "obtainotherprivilege", "string"),
        ("`impact.basemetricv2.obtainuserprivilege`",
         "boolean", "obtainuserprivilege", "string"),
        ("`impact.basemetricv2.severity`", "string",
         "`impact.basemetricv2.severity`", "string"),
        ("`impact.basemetricv2.userinteractionrequired`",
         "boolean", "userinteractionrequired", "string"),
        ("`impact.basemetricv3.cvssv3.attackcomplexity`", "string",
         "`impact.basemetricv3.cvssv3.attackcomplexity`", "string"),
        ("`impact.basemetricv3.cvssv3.attackvector`", "string",
         "`impact.basemetricv3.cvssv3.attackvector`", "string"),
        ("`impact.basemetricv3.cvssv3.availabilityimpact`", "string",
         "`impact.basemetricv3.cvssv3.availabilityimpact`", "string"),
        ("`impact.basemetricv3.cvssv3.basescore`", "double",
         "`impact.basemetricv3.cvssv3.basescore`", "string"),
        ("`impact.basemetricv3.cvssv3.baseseverity`", "string",
         "`impact.basemetricv3.cvssv3.baseseverity`", "string"),
        ("`impact.basemetricv3.cvssv3.confidentialityimpact`", "string",
         "`impact.basemetricv3.cvssv3.confidentialityimpact`", "string"),
        ("`impact.basemetricv3.exploitabilityscore`", "double",
         "`impact.basemetricv3.exploitabilityscore`", "string"),
        ("`impact.basemetricv3.impactscore`", "double",
         "`impact.basemetricv3.impactscore`", "string"),
        ("`impact.basemetricv3.cvssv3.integrityimpact`", "string",
         "`impact.basemetricv3.cvssv3.integrityimpact`", "string"),
        ("`impact.basemetricv3.cvssv3.privilegesrequired`", "string",
         "`impact.basemetricv3.cvssv3.privilegesrequired`", "string"),
        ("`impact.basemetricv3.cvssv3.scope`", "string",
         "`impact.basemetricv3.cvssv3.scope`", "string"),
        ("`impact.basemetricv3.cvssv3.userinteraction`", "string",
         "`impact.basemetricv3.cvssv3.userinteraction`", "string"),
        ("vendors", "string", "vendors", "string"),
        ("product_versions", "string", "product_versions", "string"),
        ("vendor_product_versions", "string", "vendor_product_versions", "string"),
        ("cwes", "string", "cwes", "string"),
        ("vendor_products", "string", "vendor_products", "string"),
        ("products", "string", "products", "string")


    ],
    transformation_ctx="ApplyMapping_node2",
)
partitioned_dataframe = ApplyMapping_node2.toDF().repartition(1)

uppercase_boolean: DataFrame = partitioned_dataframe.withColumn(
    "basemetricv2_acinsufinfo", initcap("basemetricv2_acinsufinfo")).withColumn(
    "obtainallprivilege", initcap("obtainallprivilege")).withColumn(
    "obtainotherprivilege", initcap("obtainotherprivilege")).withColumn(
    "obtainuserprivilege", initcap("obtainuserprivilege")).withColumn(
    "userinteractionrequired", initcap("userinteractionrequired"))


partitioned_dynamicframe = DynamicFrame.fromDF(
    uppercase_boolean, glueContext, "partitioned_df")


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://" + output_bucket + "/DocEx"},
    format_options={"separator": "\t",  "quoteChar": -1, "writeHeader": False},
    transformation_ctx="S3bucket_node3",
)

filtered = bucket.objects.filter(Prefix="DocEx/")
latest = max(filtered, key=lambda x: x.last_modified)
print(latest.key)
for object in filtered:
    if(latest.key != object.key):
        bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': object.key,

                    },
                ]
            })
bucket.meta.client.copy_object(CopySource={
    'Bucket': output_bucket,
    'Key': latest.key
},
    Bucket=output_bucket,
    Key='DocEx/DocExFinal.tsv')

bucket.delete_objects(
    Delete={
        'Objects': [
            {
                'Key': latest.key,

            },
        ]
    })


job.commit()

# CODE END
