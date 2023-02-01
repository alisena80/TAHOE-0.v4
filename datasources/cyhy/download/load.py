# IMPORTS START
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
import urllib
import boto3
# IMPORTS END

# ARG START
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_OUTPUT"])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
output_location = args["S3_OUTPUT"]
# ARG END

# CODE START
# Creates dynamic frame of existing file from data catalog


def clear_bucket():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(output_location.split("/")[2])
    bucket.objects.filter(Prefix="cyhy/").delete()


def load(col, connection):
    '''
    Loads data from mongodb
    :param col: Collection to load
    :param connection: Connection details for mongo
    '''
    read_docdb_options = {
        "uri": connection['ConnectionProperties']['CONNECTION_URL'],
        "database": "cyhy",
        "collection": col,
        "username": connection['ConnectionProperties']['USERNAME'],
        "password": connection['ConnectionProperties']['PASSWORD'],
    }
    cyhy_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="documentdb", connection_options=read_docdb_options)

    S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
        frame=cyhy_frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": output_location + "/cyhy_" + col, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="S3bucket_node3",
    )


clear_bucket()
client = boto3.client("glue")

connection = client.get_connection(Name='cyhy_connection', HidePassword=False)["Connection"]
print(connection)

load("vuln_scans", connection)
load("host_scans", connection)
load("port_scans", connection)
load("tickets", connection)
# CODE END
