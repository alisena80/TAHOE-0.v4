# IMPORTS START
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
from tahoecommon.errors import RetryError

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATA_PREFIX", "JOB_NAME", "PREPROCESS_BUCKET", "RAW_DATABASE", "TABLE_NAME", "UPDATE_LOCATION"])

data_prefix = args["DATA_PREFIX"]
job_name = args["JOB_NAME"]
preprocess_bucket = args["PREPROCESS_BUCKET"]
raw_database = args["RAW_DATABASE"]
table_name = args["TABLE_NAME"]
update_location = args["UPDATE_LOCATION"]
# ARG END


# SPARK START
spark = SparkSession.builder.config(
    'spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config(
    'spark.sql.hive.convertMetastoreParquet', 'false').config(
    "spark.sql.extensions",
    "org.apache.spark.sql.hudi.HoodieSparkSessionExtension").getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# SPARK END

# LOG START
logger = log.TahoeLogger().getSparkLogger(glueContext.get_logger())
# LOG END


# CODE START
spark.sql("set schema.on.read.enable=true")


def replace_object(updates, output_location, hudi_options):
    """
    Upserts hudi object by _id
    :param updates: Dataframe to write
    :param output_location: location of table
    :param hudi_options: hudi write options
    """
    try:

        updates.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(output_location)
    except Exception as e:
        raise RetryError(str(e))


def new_scans_table(updates, hudi_options, output_location, *join_col):
    """
    Create a new scans table does a dedup step then overwrites
    :param updates: Dataframe to insert
    :param hudi_options: hudi options to enact upsert
    :param output_location: location of table
    :param join_col: Columns to uniquely identify new record
    """

    # find latest time in updates subset
    latestChangeForEachKey = updates \
        .selectExpr(*join_col, "time") \
        .groupBy(*join_col) \
        .agg(max("time").alias("latest_time")) \
        .selectExpr(*join_col, "latest_time")
    print(f"Unique changes {latestChangeForEachKey.count()}")
    print(latestChangeForEachKey.dtypes)
    print(f"All before {updates.count()}")
    print(f"updates with true {updates.where(col('latest') == True).count()}")
    print(f"updates with false {updates.where(col('latest') == False).count()}")

    # update updates table to change old rows to false
    updatesWithFalse = updates.alias("root_updates").join(
        latestChangeForEachKey, list(join_col)).withColumn(
        "latest",
        when(
            latestChangeForEachKey.latest_time != updates.time, lit(False)).
        otherwise(lit(True))).selectExpr(
        "root_updates.*", "latest")

    print(f"All changes {updatesWithFalse.count()}")
    print(f"Latest updates with true {updatesWithFalse.where(col('latest') == True).count()}")
    print(f"Latest updates with false {updatesWithFalse.where(col('latest') == False).count()}")
    print(updatesWithFalse.dtypes)
    print("joiningBy", list(join_col))

    # Overwrite location
    updatesWithFalse.write.format("hudi"). \
        options(**hudi_options). \
        mode("overwrite"). \
        save(output_location)


def new_table(updates, hudi_options, output_location):
    '''
    New table full overwrite
    :param updates: Dataframe to insert
    :param hudi_options: Hudi options to start upsert
    :param output_location: location of table
    '''
    # over write location
    updates.write.format("hudi"). \
        options(**hudi_options). \
        mode("overwrite"). \
        save(output_location)


def scan_merge(updates, hudiDf, hudi_options, output_location, *join_col):
    '''
    Merge existing hudi table with new records
    :param updates: Dataframe to insert
    :param hudiDf: Existing table Dataframe
    :parma hudi_options:  Hudi options to start upsert
    :param output_location: location of table
    :param join_col: Columns to uniquely identify new record
    '''

    # get all hudi records that are the latest
    latestHudi = hudiDf \
        .where("hudiDf.latest = true").alias("latestHudi")

    # find latest in changes
    latestChangeForEachKey = updates \
        .selectExpr(*join_col, "time") \
        .groupBy(*join_col) \
        .agg(max("time").alias("latest_time")) \
        .selectExpr(*join_col, "latest_time")

    print(f"All changes {latestChangeForEachKey.count()}")
    print(latestChangeForEachKey.dtypes)
    print("joiningBy", list(join_col))
    print(list(join_col))

    # Compare latest in new batch with latest in existing table to see if it is a lagging record
    latestChangeForEachKey = latestHudi.join(
        latestChangeForEachKey.alias("latestInBatch"),
        list(join_col),
        "right").withColumn(
        "latest_time",
        when(latestChangeForEachKey.latest_time < latestHudi.time, lit("")).
        otherwise(latestChangeForEachKey.latest_time)).selectExpr(
        *join_col, "latest_time")

    print(f"All changes {latestChangeForEachKey.count()}")
    print(
        f"Lagging records {latestChangeForEachKey.where(col('latest_time') == '').count()}")
    print(latestChangeForEachKey.dtypes)
    print("joiningBy", list(join_col))

    # update updates table to change old rows to false
    updatesWithFalse = updates.alias("root_updates").join(
        latestChangeForEachKey, list(join_col)).withColumn(
        "latest",
        when(
            latestChangeForEachKey.latest_time != updates.time, lit(False)).
        otherwise(lit(True))).selectExpr(
        "root_updates.*", 'latest').alias("updates")

    print(f"All inserts {updatesWithFalse.count()}")
    print(
        f"Latest inserts with true {updatesWithFalse.where(col('latest') == True).count()}")
    print(
        f"Latest inserts with false {updatesWithFalse.where(col('latest') == False).count()}")
    print("joiningBy", list(join_col))

    # Find df require updating
    newRecordsChanged = latestHudi \
        .join(updatesWithFalse.alias("updates"), list(join_col)) \
        .where("updates.latest = true AND updates._id <> latestHudi._id AND updates.time > latestHudi.time") \
        .selectExpr("latestHudi.*") \
        .withColumn("latest", lit(False))
    print("Existing table updates count", newRecordsChanged.count())
    newRecordsChanged.printSchema()

    # union updates and new inserts
    stagedUpdates = updatesWithFalse.alias("all_updates").unionByName(
        newRecordsChanged, allowMissingColumns=True)
    print("staged schema")
    stagedUpdates.printSchema()
    print("Staged updates count", stagedUpdates.count())
    try:
        stagedUpdates.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(output_location)
    except Exception as e:
        raise RetryError(str(e))

    # hudi upsert


changes = False

print(table_name)
print("Processing", update_location.split(","))
updates: DataFrame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": update_location.split(","), "recurse": True},
    format="json"
).resolveChoice(specs=[('ip_int', 'cast:int')]).toDF()

print("Updates count", updates.count())
print(updates.dtypes)

hudiDf = None

if "last_change" in updates.columns:
    precombine = "last_change"
elif "time" in updates.columns:
    precombine = "time"
else:
    precombine = "_id"

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.precombine.field': precombine,
    'hoodie.datasource.write.recordkey.field': '_id',
    'hoodie.parquet.compression.codec': 'snappy',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.consistency.check.enabled': 'true',
    'hoodie.datasource.hive_sync.database': raw_database,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.meta.sync.enable': 'true',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    'hoodie.parquet.compression.ratio': '0.95',
    'hoodie.meta.sync.client.tool.class': 'org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool',
    'hoodie.clean.automatic': 'true',
    'hoodie.clean.async': 'true',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': '2'

}

table_location = f"s3://{preprocess_bucket}/{data_prefix}/{table_name}"
try:
    hudiDf = spark. \
        read. \
        format("hudi"). \
        load(table_location).alias("hudiDf")

    print("Table count", hudiDf.count())
except Exception as e:
    if "java.io.FileNotFoundException" in str(e):
        print("new table")
        if "port_scans" in table_name or "vuln_scans" in table_name:
            # New  tableon port, ip, and protocol
            new_scans_table(
                updates, hudi_options, table_location, "port", "ip",
                "protocol")
        elif "host_scans" in table_name:
            # New table on port, ip, and protocol
            new_scans_table(updates, hudi_options, table_location, "ip")
        else:
            # Default to insert alll data
            new_table(updates, hudi_options, table_location)
        changes = True

if hudiDf is not None and hudiDf.count() == 0:
    raise RetryError("Table is empty but created.")
if hudiDf is not None and updates.count() > 0:
    if "port_scans" in table_name or "vuln_scans" in table_name:
        # Upsert on port, ip, and protocol
        scan_merge(updates, hudiDf, hudi_options,
                   table_location, "port", "ip", "protocol")
    elif "host_scans" in table_name:
        # upsert only based on ip
        scan_merge(updates, hudiDf, hudi_options, table_location, "ip")
    else:
        # Default to full replace on object id
        replace_object(updates, hudiDf, table_location, hudi_options)
    changes = True

if not changes:
    raise ValueError("No changes found")
else:
    logger.dblog(log.StatusLog(f"{data_prefix}_{table_name}", log.Status.PREPROCESSED))

job.commit()

# CODE END
