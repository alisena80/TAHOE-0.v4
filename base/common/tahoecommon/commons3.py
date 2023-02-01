# Common util functions for S3
# These functions are to be called inside Lambda on AWS

import os
import io
import boto3
from botocore.errorfactory import ClientError

import gzip
import zipfile
import tarfile


def s3_upload(localpath: str, s3_bucket: str, s3_path: str):
    """
    Upload file.

    :param localpath: Local path to upload from
    :param s3_bucket: S3 bucket name
    :param s3_path: S3 Key
    
    """
    print("s3_upload-----", localpath, s3_bucket, s3_path)
    s3 = boto3.resource('s3')
    response = s3.meta.client.upload_file(localpath, s3_bucket, s3_path)
    return response


def s3_download(localpath, s3_bucket, s3_path):
    """
    Download file.

    :param localpath: Local path to upload from
    :param s3_bucket: S3 bucket name
    :param s3_path: S3 Key

    """
    print("s3_download-----", localpath, s3_bucket, s3_path)
    s3 = boto3.resource('s3')
    response = s3.Bucket(s3_bucket).download_file(s3_path, localpath)
    return response


def s3_checkfileexist(s3_bucket, s3_path):
    """
    Check if file key exists in s3.

    :param s3_bucket: S3 bucket name
    :param s3_path: S3 Key
    """
    print("s3_checkfileexist-----", s3_bucket, s3_path)
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_path)
        return True
    except ClientError:
        return False


def s3_createDirIfNotExist(s3_bucket, s3_path):
    """
    Check if dir key exists in s3.

    :param s3_bucket: S3 bucket name
    :param s3_path: S3 Key
    """
    print("s3_createDirIfNotExist-----", s3_bucket, s3_path)
    s3 = boto3.client('s3')
    exist = s3_checkfileexist(s3_bucket, s3_path)
    if exist == False:
        try:
            s3.put_object(Bucket=s3_bucket, Body='', Key=s3_path)
            return True
        except ClientError:
            return False
    return True


def s3_unzipfile(s3_bucket, s3_zipfilepath, s3_unzipdir):
    """
    Unzip file to local path and upload to s3

    :param s3_bucket: S3 bucket name
    :param s3_zipfilepath: File path directory to unzip to
    :param s3_unzipdir: Directory name where to unzip to
    """

    print("s3_unzipfile-----", s3_bucket, s3_zipfilepath, s3_unzipdir)
    s3 = boto3.resource('s3')
    zip_obj = s3.Object(bucket_name=s3_bucket, key=s3_zipfilepath)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())

    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        print("s3_unzipfile----- chunk ", filename)
        file_info = z.getinfo(filename)
        s3.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=s3_bucket,
            Key=s3_unzipdir + filename
        )


def s3_ungzipfile(s3_bucket, s3_zipfilepath, s3_unzipfilepath):
    """
    Unzip file to local path and upload to s3 using gzip

    :param s3_bucket: S3 bucket name
    :param s3_zipfilepath: File path directory to unzip to
    :param s3_unzipdir: Directory name where to unzip to
    """
    print("s3_ungzipfile-----", s3_bucket, s3_zipfilepath, s3_unzipfilepath)

    s3 = boto3.client('s3')
    inputobj = s3.get_object(Bucket=s3_bucket, Key=s3_zipfilepath)

    inputcontent = inputobj['Body'].read()
    thefileobj = io.BytesIO(inputcontent)
    fobj = gzip.GzipFile(None, 'rb', fileobj=thefileobj)
    s3.upload_fileobj(Fileobj=fobj, Bucket=s3_bucket, Key=s3_unzipfilepath)


def s3_untarfile(s3_bucket, s3_zipfilepath, s3_unzipdir):
    """
    Unzip file to local path and upload to s3 using tar

    :param s3_bucket: S3 bucket name
    :param s3_zipfilepath: File path directory to unzip to
    :param s3_unzipdir: Directory name where to unzip to
    """
    print("s3_untarfile-----", s3_bucket, s3_zipfilepath, s3_unzipdir)
    s3 = boto3.client('s3')
    input_tar_file = s3.get_object(Bucket=s3_bucket, Key=s3_zipfilepath)

    input_tar_content = input_tar_file['Body'].read()
    uncompressed_key = s3_unzipdir
    # errorMessage": "a bytes-like object is required, not '_io.BytesIO'",
    with tarfile.open(fileobj=io.BytesIO(input_tar_content)) as tar:
        for tar_resource in tar:
            if (tar_resource.isfile()):
                inner_file_bytes = tar.extractfile(tar_resource).read()
                s3.meta.client.upload_fileobj(BytesIO(bytes_content), Bucket=s3_bucket, Key=uncompressed_key)


def s3_deleteAllFiles(s3_bucket, s3_path_prefix):
    """
    Deletes ALL directories and files that match the prefix.

    :param s3_bucket: S3 bucket name
    :param s3_path_prefix: path to delete
    """
    print("s3_deleteAllFiles-----", s3_bucket, s3_path_prefix)
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_path_prefix)
    if "Contents" in response:
        for object in response['Contents']:
            print('  Deleting', object['Key'])
            s3.delete_object(Bucket=s3_bucket, Key=object['Key'])
