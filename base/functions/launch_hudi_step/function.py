
import json
import os
import boto3
SF_ARN = os.environ['STEP_FUNCTION']
SQS_URL = os.environ['QUEUE_URL']
DATA_PREFIX = os.environ["DATA_PREFIX"]
sf = boto3.client("stepfunctions")
sqs = boto3.client('sqs')


def handler(event, context):
    '''
    Launches incremental update stepfunctions. Step functions handle retry
    '''
    # look at cyhy ingestion step function if busy with input containing sub folder
    print("event", event)
    message = [None]
    updated_tables = {}
    while True:
        # Recieve messages from queue
        message = sqs.receive_message(QueueUrl=SQS_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        print(message)
        if "Messages" not in message:
            break
        print(message)
        # Get list of messages from sqs
        for x in message['Messages']:
            s3_records = json.loads(x["Body"])["Records"] if "Records" in json.loads(x["Body"]) else None
            if s3_records is None:
                print(f"The record {x} does not match the expected input")
                continue
            # Get all s3 records in message
            for records in s3_records:
                table_name = records["s3"]["object"]["key"].split("/")[-2]
                if table_name not in updated_tables and records['s3']['object']['key'].split("/")[-1] is not "":
                    updated_tables[table_name] = set()
                if records['s3']['object']['key'].split("/")[-1] is not "":
                    # Add location of to table name
                    updated_tables[table_name].add(
                        f"s3://{records['s3']['bucket']['name']}/{records['s3']['object']['key']}")
        # Delete processed messages
        response = sqs.delete_message_batch(QueueUrl=SQS_URL, Entries=list(
            map(lambda x: {'Id': x['MessageId'], 'ReceiptHandle': x['ReceiptHandle']}, message['Messages'])))
    print("sf_input", updated_tables)
    # Start step functions one for each table being written
    for x in updated_tables:
        sf.start_execution(stateMachineArn=SF_ARN, input=json.dumps(
            {"update_location": ",".join(updated_tables[x]),
                                "data_prefix": DATA_PREFIX, "table_name": x}))



                



    

