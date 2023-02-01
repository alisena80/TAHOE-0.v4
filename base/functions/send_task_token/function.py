
from http import client
import boto3
import json
sf = boto3.client("stepfunctions")

def handler(event, context):
    '''
    Sends a task token in order to resume a pipline waiting or human approval
    :param event: - information regarding the output to pull from
    :param context:
    :return:
    '''
    if event["action"] == "fail":
        response = sf.send_task_failure(
            taskToken='string',
            error='rejected',
            cause='rejected'
        )
    elif event["action"] == "continue":
        response = sf.send_task_success(
            taskToken=event["taskToken"],
            output=json.dumps({"str":"test"})
    )