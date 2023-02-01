# IMPORTS START
import awswrangler as wr
import boto3
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from opensearchpy import OpenSearch
from pandas import json_normalize, concat, DataFrame
import json
from datetime import datetime

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DETAILS", "EXECUTION", "JOB_NAME", "QUERY"])

details = args["DETAILS"]
execution = args["EXECUTION"]
job_name = args["JOB_NAME"]
query = args["QUERY"]
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
try:
    query = json.loads(query)
except json.JSONDecodeError:
    raise ValueError("Query failed to parse")

try:
    additional_details = json.loads(details)
except json.JSONDecodeError:
    raise ValueError("Query failed to parse")

print("QUERY", query)
print("EXECUTION", execution)


def get_secret():
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId='opensearch_host')
    if 'SecretString' in secret:
        secret = json.loads(secret['SecretString'])
    return secret


def write_dynamic_frame(frame: DataFrame):
    if not frame.empty:
        remove_source(frame)
        frame = frame.replace({'true': True, 'false': False})
        # print(wr.catalog.extract_athena_types(df=frame))
        res = wr.s3.to_parquet(
            df=frame, path=additional_details['location'],
            dataset=True, compression="snappy",
            dtype={"aware.errors.findings": "string",
                   "quality.datatype_error_findings": 'array<struct<field:string,message:string,value:string>>'})


def connect():
    secret = get_secret()
    host = secret["HOST"]
    port = secret["PORT"]

    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False)
    return client


def remove_source(frame: DataFrame):
    '''
    renames _source from column names to remove _source

    :param frame: frame to rename
    '''
    frame.rename(mapper=lambda col: col[len("_source."):]
                 if col.startswith("_source.") else col, inplace=True, axis=1)


def search_and_scroll(client: OpenSearch):
    data = []
    print(str(query))

    record_count = 0
    # get the maximum amount of values possible from elasticsearch api
    response = client.search(
        body=query["query"],
        index=query["index"],
        size=10000,
        scroll='10m'
    )
    print("Total Records", response['hits']['total']['value'])
    if response['hits']['total']['value'] == 0:
        return DataFrame()

    while True:
        # normalize result into dataframe
        pd = json_normalize(response['hits']['hits'])
        data.append(pd)
        record_count += len(pd)
        print("Records Processed", record_count)

        if record_count != response['hits']['total']['value']:
            response = client.scroll(
                scroll_id=response['_scroll_id'],
                scroll='10m'
            )
        else:
            break
    return concat(data)


connection = connect()

write_dynamic_frame(search_and_scroll(connection))
logger.notify(log.NotifyQueryLog("cdm", query, additional_details))
job.commit()

# CODE END
