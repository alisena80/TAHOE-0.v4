# IMPORTS START
#
# NIST data download :
#   NVD - https://nvd.nist.gov/feeds/json/cve/1.1/${filename}.gz
#   CVE
#   CPE
#   CPEMatch - https://nvd.nist.gov/feeds/json/cpematch/1.0/nvdcpematch-1.0.json.gz
# Bucket
#       s3://tahoe/data/nist

from tahoecommon import commonfile
from tahoecommon import commonhttp
from tahoecommon import commons3
import inspect
import os
import gzip
import shutil

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
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)


print("---------------------------------START")
print("currenttime ", currenttime)
print("parentdir ", parentdir)
print("currentdir", currentdir)


def main():

    s3_basepath_raw = data_prefix.replace("_", "/") + '/'
    tmpdir = '/tmp/'

    commons3.s3_createDirIfNotExist(s3_bucket, s3_basepath_raw)

    download_nvdcve(base_url, s3_bucket, s3_basepath_raw, tmpdir, True)
    #download_nvdcpe(s3_bucket, s3_basepath_raw, tmpdir, True)

    currenttime = commonutil.getdaytime()
    print("currenttime ", currenttime)
    logger.dblog(log.StatusLog(data_prefix, log.Status.DOWNLOADED))
    print("---------------------------------DONE")


def download_nvdcve(baseurl, s3_bucket, s3_basepath_raw, tmpdir, doupload):
    '''
    Downloads all nvd years
    :param baseurl: Url to get nvd data from
    :param s3_bucket: S3 bucket to upload to
    :param s3_basepath_raw: S3 prefix to upload to
    :param tmpdir: Glue tmp write
    :param doupload: upload to s3 flag
    '''

    print("download_cve ---------------------------------START")

    # NVD
    # https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2021.meta
    # https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2021.json.gz
    currentyear = commonutil.getCurrentYear()
    for year in range(2002, currentyear+1):
        doDownload_nvdcve(baseurl, s3_bucket, s3_basepath_raw,
                          str(year), tmpdir, doupload)
    doDownload_nvdcve(baseurl, s3_bucket, s3_basepath_raw,
                      'recent', tmpdir, doupload)


def doDownload_nvdcve(baseurl, s3_bucket, s3_basepath_raw, year, tmpdir, doupload):
    '''
    Downloads a certain year of nvd cve data
    :param baseurl: Url to get nvd data from
    :param s3_bucket: S3 bucket to upload to
    :param s3_basepath_raw: S3 prefix to upload to
    :param year: Year to download
    :param tmpdir: Glue tmp write
    :param doupload: upload to s3 flag
    '''
    print('    checking - year: ' + year)
    isneeddownload = checkIfDownloadNeeded(
        baseurl, s3_bucket, s3_basepath_raw, year, tmpdir)
    if isneeddownload == True:
        filename_meta = 'nvdcve-1.1-' + year + '.meta'
        filename_gz = 'nvdcve-1.1-' + year + '.json.gz'
        filename_json = 'nvdcve-1.1-' + year + '.json'

        print('    downloading - filename: ' + filename_gz)
        url = baseurl + filename_gz
        local_path = tmpdir + filename_gz
        local_path_json = tmpdir + filename_json
        commonhttp.httpdownload_stream(url, local_path)

        # unzip file
        gunzipFile(local_path, local_path_json)

        s3_path = s3_basepath_raw + filename_json
        if doupload == True:
            commons3.s3_upload(local_path_json, s3_bucket, s3_path)


def download_nvdcpe(s3_bucket, s3_basepath_raw, tmpdir, doupload):
    '''
    Downloads a certain year of nvd cpe data
    :param s3_bucket: S3 bucket to upload to
    :param s3_basepath_raw: S3 prefix to upload to
    :param tmpdir: Glue tmp write
    :param doupload: upload to s3 flag
    '''
    print("download_cpe ---------------------------------START")
    baseurl = 'https://nvd.nist.gov/feeds/json/cpematch/1.0/'
    filename = 'nvdcpematch-1.0.json.gz'
    url = baseurl + filename
    s3_path = s3_basepath_raw + filename
    local_path = tmpdir + filename
    print('    downloading - filename: ' + filename)
    commonhttp.httpdownload_stream(url, local_path)
    if doupload == True:
        commons3.s3_upload(local_path, s3_bucket, s3_path)

# Check if this year need to be downloaded by comparing the diff the in the meta file


def checkIfDownloadNeeded(url, s3_bucket, s3_basepath_raw, year, tmpdir):
    '''
    Checks metadata for changes
    :param url: Url to get nvd data from
    :param s3_bucket: S3 bucket to upload to
    :param s3_basepath_raw: S3 prefix to upload to
    :param year: Year to download
    :param tmpdir: Glue tmp write
    '''
    # Files in S3 are unzip
    filename_meta = 'nvdcve-1.1-' + str(year) + '.meta'
    filename_gz = 'nvdcve-1.1-' + str(year) + '.json'

    # Download new meta
    commonhttp.httpdownload_stream(url + filename_meta, tmpdir + filename_meta)

    # check if current meta exist in s3 bucket
    existmeta = commons3.s3_checkfileexist(
        s3_bucket, s3_basepath_raw + filename_meta)
    if existmeta == False:
        # put new meta into s3 bucket - return need download
        commons3.s3_upload(tmpdir + filename_meta, s3_bucket,
                           s3_basepath_raw + filename_meta)
        return True

    if year == "recent":
        year = commonutil.getCurrentYear()
    existgz = commons3.s3_checkfileexist(
        s3_bucket, s3_basepath_raw + "year=" + str(year) + "/" + filename_gz)
    if existgz == False:
        # put new meta into s3 bucket - return need download
        commons3.s3_upload(tmpdir + filename_meta, s3_bucket,
                           s3_basepath_raw + filename_meta)
        # No download yet for this year - return need download
        return True

    # both meta & gz exist, need to check if metat diff
    # Get the current meta from s3 bucket to tmp dir
    commons3.s3_download(tmpdir + filename_meta + ".last",
                         s3_bucket, s3_basepath_raw + filename_meta)

    # check diff between new and current
    isdiff = commonfile.compareFiles(
        tmpdir + filename_meta, tmpdir + filename_meta + ".last")
    if isdiff == True:
        commons3.s3_upload(tmpdir + filename_meta, s3_bucket,
                           s3_basepath_raw + filename_meta)
    return isdiff


def gunzipFile(gzfilepath, outfilepath):
    '''
    Zips file
    :param gzfilepath: File to unzip
    :param outfilepath: Output of unziped
    '''
    print('    gunzipFile - filename: ' + gzfilepath)
    with gzip.GzipFile(gzfilepath, 'rb') as f_in:
        with open(outfilepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


main()

# CODE END
