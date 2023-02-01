import json
import sys
import os
import base64
import gzip
import boto3
from datetime import datetime
import os
import ast
import re
from tahoelogger import log
dynamodb = boto3.resource('dynamodb')
dynamodbc = boto3.client('dynamodb')
default_sns = os.environ["DEFAULT_SNS"]
table_name = os.environ["TABLE_NAME"]
table = dynamodb.Table(table_name)
sns = boto3.client("sns")
def handler(event, context):
    print(event)

    with table.batch_writer() as batch:
        for record in event["Records"]:
            print(extract_data(record["kinesis"]["data"]))
            data=json.loads(extract_data(record["kinesis"]["data"]))
            print(data)
            for logs in data["logEvents"]:
                print(isolate_log(logs["message"]))
                print("logs", logs)
                message = ast.literal_eval(isolate_log(logs["message"]))
                class_type = message["type"]
                _class = getattr(log, class_type)
                if message["level"] == "DBLOG":
                    dynamo_record = format_log_to_dynamo(message, _class)
                    batch.put_item(Item=dynamo_record)
                elif message["level"] == "NOTIFY":
                    body = format_log_to_notify(message, _class)
                    sns.publish(**body)
                    print("NOTIFYING")
                     

def extract_data(data):
    decodedBytes = base64.b64decode(data)
    decompressed = gzip.decompress(decodedBytes).decode()
    return decompressed

def isolate_log(data):
    match = data
    matches = re.findall(r"{.*}", data)
    print(matches)
    if len(matches) == 1:
        match = matches[0]
    return match
    
def format_log_to_dynamo(record, _class):
    obj = _class.from_dict(ast.literal_eval(record["message"]))
    if obj.is_transactional():
        obj.set_client(dynamodbc)
        obj.lock(table_name)
        obj.lock_task()
        obj.unlock(table_name)
    else:
        return obj.dynamo(record["timestamp"])

def format_log_to_notify(record, _class):
    """
    Notify to specified sns or use default sns
    """
    notify_details = _class.from_dict(ast.literal_eval(record["message"]))
    if notify_details.sns is None:
        notify_details.sns = default_sns
    return notify_details.format()

