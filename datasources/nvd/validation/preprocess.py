# IMPORTS START
import os
import json
import boto3
import time
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
print("DATASOURCE =  ", datasource)


def extractCVEItems(filep_in, filep_out):
	'''
	Unnest data from CVE array
	:param filep_in: File in
	:param filep_out: File out
	'''
	with open(filep_in, "r") as fin, open(filep_out, "w") as fout:
		old_json = json.load(fin)
		new_json = json.dumps(old_json["CVE_Items"], separators=(',', ':'))
		fout.write(new_json)


def doPreproc():
	'''
	Preprocess all NVD records by removing CVE array
	'''
	print("Preprocess raw data for ", datasource)
	s3 = boto3.client('s3')
	response = s3.list_objects_v2(Bucket=s3_raw, Prefix=datasource)
	if "Contents" in response:
		for object in response['Contents']:
			filepath = object['Key']
			if filepath.endswith(".json"):
				lastindex = filepath.rindex('/')
				filename = filepath[lastindex+1:]
				filename_local = "/tmp/" + filename
				print('  doPreproc', filepath, filename, filename_local)
				s3.download_file(s3_raw, filepath, filename_local)
				extractCVEItems(filename_local, filename_local + ".pre")
				s3.upload_file(filename_local + ".pre", s3_preproc,
                                    datasource + "/" + filename)
				os.remove(filename_local)
				os.remove(filename_local + ".pre")


# 1. PreProc - extract the CVE_Items array only
doPreproc()


# 3. clean interim
print("Clean Interim data for ", datasource)
logger.dblog(log.StatusLog(datasource, log.Status.PREPROCESSED))
commons3.s3_deleteAllFiles(s3_interim, datasource)

# CODE END
