import boto3
import os
import json
import datetime

sns = boto3.client('sns')
email_topic = os.environ['SNS_TOPIC']


def handler(event, context):
    """
    Analyzes the output of the job and sends an email with the error if required

    :param event: - information regarding the output to pull from
    :param context:
    :return:
    """

    details = event["details"]

    # If the job failed, prepare a message with the error
    if "errors" in details:
        message = "Error occurred while processing!\n\n"

        # The cause is stored as the string representation of a json string and needs to be parsed out
        error_details = json.loads(details["errors"]["Cause"])
        message += "Job " + error_details["JobName"] + " failed with error " + \
                   details["errors"]["Error"] + ":\n\n" + \
            error_details["ErrorMessage"]
        send_email(message)

        # Throw an exception so that the step function reports a failure
        raise Exception(message)


def send_email(message):
    # Publish the email message to the existing SNS topic
    sns.publish(
        TopicArn=email_topic,
        Message=message
    )


def dynamo_write(event):
    # update = {
    #     "PK": {"S": event["dataPrefix"]},
    #     "SK": {"S": "LATEST"},
    #     "Status": {"S":event["status"]},
    #     "TaskToken": {"S": event["taskToken"]} if "taskToken" in event else {"S": "None"}
    # }

    log = {
        "PK": {"S": event["dataPrefix"]},
        "SK": {"S": "LOG#" + str(datetime.datetime.now())},
        "Status": {"S": "FAILED"},
    }
    client.batch_write_item(
        RequestItems={
            'pipeline-tracking': [
                {
                    'PutRequest': {
                        'Item': log
                    }

                }]
        })
