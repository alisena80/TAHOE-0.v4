# IMPORTS START
import boto3
from awsglue.transforms import *
from pyspark.sql.dataframe import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat_ws, collect_set, concat, split, col, lit

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
def get_nvd_data_catalog(table):
    # Script generated for node S3 bucket
    return glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx=table,
    )


def count_children(key):
   return key.count("children") * 2 + key.count("cpe")


def create_column_name(key):
    return ".".join(key.split("_")[1:]).replace("cpe.match", "cpe_match")


def assign_join_order(cpe_match_table_names):
    # key to join on : dataframe
    properties = {}
    for x in cpe_match_table_names:
        properties[create_column_name(x["Name"])] = get_nvd_data_catalog(
            x["Name"]).toDF()
    print(properties)
    return sorted(properties.items(), key=lambda x: count_children(x[0]))


def concat_columns_filter(columns, filter):
    return [col(column) for column in columns if filter in column]


nvd_configuration_node: DataFrame = get_nvd_data_catalog(
    stack_prefix + "nvd_configurations_nodes").toDF()
glue = boto3.client("glue")
cpe_match_table_names = glue.get_tables(
    DatabaseName=database,
    Expression=".*val_children|.*cpe_match")["TableList"]
cpe_match_tables = assign_join_order(cpe_match_table_names)

print(cpe_match_tables)

# rename main id colum to id name easily
nvd_configuration_node = nvd_configuration_node.withColumnRenamed(
    "id", "mainId")
nvd_configuration_node = nvd_configuration_node.withColumnRenamed(
    "index", "mainIndex")


# join all required tables extracted by name
for joins in cpe_match_tables:
    nvd_configuration_node = nvd_configuration_node.join(
        joins[1], nvd_configuration_node["`" + joins[0] + "`"] == joins[1]["`id`"], how="left").drop("index", "id")
nvd_configuration_node.printSchema()
print(nvd_configuration_node.count())
nvd_configuration_node.show()

cpe_columns = list(filter(lambda name: "cpe23Uri" in name,
                   nvd_configuration_node.columns))

# extract all vendor/product/version info from cpe23Uri col
for x, i in enumerate(cpe_columns):
    split_col = split(nvd_configuration_node["`" + i + "`"], "[:]")
    print(split_col.getItem(3))
    nvd_configuration_node = nvd_configuration_node.select("*", split_col.getItem(3).alias("single_vendors" + str(x)), split_col.getItem(4).alias("single_product" + str(x)),
                                                           split_col.getItem(5).alias(
                                                               str(x) + "version"),
                                                           concat(split_col.getItem(3), lit("_"), split_col.getItem(
                                                               4)).alias("vendor_products" + str(x)),
                                                           concat(split_col.getItem(4), lit("_"),  split_col.getItem(
                                                               5)).alias("products_versions" + str(x)),
                                                           concat(split_col.getItem(3), lit("_"), split_col.getItem(4), lit("_"), split_col.getItem(5)).alias("vendor_product_versions" + str(x)))

nvd_configuration_node.show()
nvd_configuration_node.printSchema()

# Group all mainId's and collect all the different unique vendors/product/versions columns
aggregations = [collect_set("`" + columns + "`").alias(columns)
                for columns in nvd_configuration_node.columns if "vendor" in columns or "product" in columns or "version" in columns]
nvd_configuration_node = nvd_configuration_node.groupBy(
    "mainId").agg(*aggregations)
filterable_columns = nvd_configuration_node.columns
# merge all columns of same type together
nvd_configuration_node = nvd_configuration_node.select("mainId", concat_ws("|", *concat_columns_filter(filterable_columns, "single_vendor")).alias("vendors"),
                                                       concat_ws(
                                                           "|", *concat_columns_filter(filterable_columns, "single_product")).alias("products"),
                                                       concat_ws(
                                                           "|", *concat_columns_filter(filterable_columns, "vendor_products")).alias("vendor_products"),
                                                       concat_ws("|", *concat_columns_filter(filterable_columns,
                                                                                             "products_versions")).alias("product_versions"),
                                                       concat_ws("|", *concat_columns_filter(filterable_columns, "vendor_product_versions")).alias("vendor_product_versions"))
nvd_configuration_node.show()
nvd_configuration_node.printSchema()

nvd_vendor = DynamicFrame.fromDF(
    nvd_configuration_node, glueContext, "nvd_configuration_node")
# Script generated for node S3 bucket
output = glueContext.write_dynamic_frame.from_options(
    frame=nvd_vendor,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": s3_output},
    format_options={"compression": "snappy"},
    transformation_ctx="output"
)

# CODE END
