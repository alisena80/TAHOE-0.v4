# IMPORTS START
#
# Mitre data download :
#
# CWE
#   Software - https://cwe.mitre.org/data/csv/699.csv.zip
#   Hardware - https://cwe.mitre.org/data/csv/1194.csv.zip
#   Design   - https://cwe.mitre.org/data/csv/1000.csv.zip
# Bucket
#       s3://tahoe/data/mitre
from tahoecommon import commonhttp
from tahoecommon import commons3
from email.mime import base
import zipfile
import boto3
import json
import os
import inspect

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from tahoecommon import commonutil
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["BASE_URL", "DATA_PREFIX", "S3_BUCKET", "continuous-log-logGroup"])

base_url = args["BASE_URL"]
data_prefix = args["DATA_PREFIX"]
s3_bucket = args["S3_BUCKET"]
continuous_log_loggroup = args["continuous_log_logGroup"]
# ARG END




# LOG START
currenttime = commonutil.getdaytime()
logger = log.TahoeLogger().getPyshellLogger(continuous_log_loggroup, currenttime)
# LOG END


# CODE START

# download from site
def download_cwe(s3_bucket, s3_basepath_raw, tmpdir, doupload):
    '''
    Downloads mitre cves

    :param s3_bucket: S3 Bucket to download to 
    :param s3_basepath_raw: S3 datasource path
    :param tmpdir: Glue local temp dir
    :param doupload: Upload to s3 flag
    '''
    print("download_cwe ---------------------------------START")
    for f in ('699', '1000', '1026', '1194', '1350'):
        filename = f + '.csv.zip'
        local_path = tmpdir + filename
        url = base_url + filename

        print('    downloading - filename: ' + filename)
        commonhttp.httpdownload_stream(url, local_path)

        unzip_filename = f + '.csv'
        unzip_local_path = tmpdir + unzip_filename

        print('    unzip - ' + local_path)
        with zipfile.ZipFile(local_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir)

            # convert to UTF-8
            print('    convert UTF-8 - ' + unzip_local_path)
            utf8__filename = f + '.csv.utf8'
            utf8__local_path = tmpdir + utf8__filename
            with open(unzip_local_path, 'rb') as source_file:
                with open(utf8__local_path, 'wb') as dest_file:
                    contents = source_file.read()
                    dest_file.write(contents.decode('us-ascii', 'replace').encode('utf-8'))
                    # dest_file.write(contents.decode('utf-16').encode('utf-8'))
                    # dest_file.write(contents.decode('latin-1').encode('utf-8'))

            s3_path = s3_basepath_raw + unzip_filename
            print('    upload S3 - : ' + utf8__local_path + ' to ' + s3_path)
            if doupload == True:
                commons3.s3_upload(utf8__local_path, s3_bucket, s3_path)


print("---------------------------------START")
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
print("currenttime ", currenttime)
print("parentdir ", parentdir)
print("currentdir", currentdir)

s3_basepath_raw = data_prefix.replace("_", "/") + '/'
tmpdir = '/tmp/'

print("s3_bucket ", s3_bucket)

commons3.s3_createDirIfNotExist(s3_bucket, s3_basepath_raw)
download_cwe(s3_bucket, s3_basepath_raw, tmpdir, True)


logger.dblog(log.StatusLog(data_prefix, log.Status.DOWNLOADED))

currenttime = commonutil.getdaytime()
print("currenttime ", currenttime)
print("---------------------------------DONE")

# CODE END
