# IMPORTS START
import os
import json
import time
import boto3
import json
import csv
from botocore.errorfactory import ClientError
import inspect
from tahoecommon import commons3

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from tahoecommon import commonutil
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DATASOURCE", "S3_INTERIM", "S3_PREPROC", "S3_RAW", "continuous-log-logGroup"])

datasource = args["DATASOURCE"]
s3_interim = args["S3_INTERIM"]
s3_preproc = args["S3_PREPROC"]
s3_raw = args["S3_RAW"]
continuous_log_loggroup = args["continuous_log_logGroup"]
# ARG END




# LOG START
currenttime = commonutil.getdaytime()
logger = log.TahoeLogger().getPyshellLogger(continuous_log_loggroup, currenttime)
# LOG END


# CODE START
# 1. Count csv header colmn and trim each record up to the column count

datasource = datasource.replace("_", "/")
def trimCSVColumns(filep_in, filep_out):
    '''
    Trim columns to match data columns to be read by 
    '''

    with open(filep_in, "r") as fin, open(filep_out, "w") as fout:
        reader = csv.reader(fin,  delimiter=',', quotechar='"')
        writer = csv.writer(fout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        totalrows = 0
        rownum = 0
        totalheadercolumns = 0
        for row in reader:
            rownum = rownum+1
            if rownum == 1:
                # Count the number of columns
                for col in row:
                    totalheadercolumns = totalheadercolumns + 1
                print("HEADER", "totalheadercolumns", totalheadercolumns)
                writer.writerow(row)
            elif rownum <= 1000000:
                totalrows = totalrows + 1
                thisrowcolumns = len(row)
                if (thisrowcolumns > totalheadercolumns):
                    # delete the last column
                    del row[thisrowcolumns-1]
                writer.writerow(row)

    fin.close()
    fout.close()
    print("    totalrows", totalrows)


def doPreproc():
    print("Preprocess raw data for ", datasource)

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=s3_raw, Prefix=datasource)
    print(response)
    if "Contents" in response:
        for object in response['Contents']:
            filepath = object['Key']
            if filepath.endswith(".csv"):
                lastindex = filepath.rindex('/')
                filename = filepath[lastindex+1:]
                filename_local = "/tmp/" + filename
                print('  dpPreproc', filepath, filename, filename_local)

                s3.download_file(s3_raw, filepath, filename_local)

                trimCSVColumns(filename_local, filename_local + ".pre")
                s3.upload_file(filename_local + ".pre", s3_preproc, datasource + "/" + filename)


# 3. Clean old interim data
def doCleanInterim():
    print("Clean Interim data for ", datasource)
    commons3.s3_deleteAllFiles(s3_interim, datasource)


currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
print("---------------------------------START")
print("parentdir ", parentdir)
print("currentdir", currentdir)

doPreproc()
logger.dblog(log.StatusLog(datasource.replace("/", "_"), log.Status.PREPROCESSED))
doCleanInterim()

# CODE END
