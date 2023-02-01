import json
import boto3
import os 
SF_ARN = os.environ['STEP_FUNCTION']
DATA_PREFIX = os.environ['DATA_PREFIX']
sf = boto3.client('stepfunctions')

def handler(event, context):
    '''
    Launches the consumer step function and passes on interim sns context to input
    :param event: - information regarding the output to pull from
    :param context:
    :return:

    '''
    sf.start_execution(
        stateMachineArn=SF_ARN,
        input=json.dumps({"input": json.loads(event["Records"][0]["Sns"]["Message"])})
    )
