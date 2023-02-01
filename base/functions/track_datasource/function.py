from http import client
import os
from telnetlib import STATUS
import boto3
import os
import datetime
import json
from tahoelogger import log
client = boto3.client('dynamodb')
sns = boto3.client('sns')
SNS_TOPIC = os.environ['SNS_TOPIC']
logger = log.TahoeLogger().getStandardLogger()


def handler(event, context):
    """
    Introduces basic source health tracking in dynamo database
    TODO Explore other logging machanisms

    :param event: - information regarding the output to pull from
    :param context:
    :return:
    """
    logger.dblog(log.StatusLog(event["dataPrefix"], log.Status.FROZEN, event["taskToken"]))
    logger.notify(log.NotifyLog(
        event["dataPrefix"],
        f"The pipeline is halted due to a schema change in {event['dataPrefix']}",
        f"{event['dataPrefix']} halted"))
