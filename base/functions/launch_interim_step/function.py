import json
import boto3
import os 
SF_ARN = os.environ['STEP_FUNCTION']
DATA_PREFIX = os.environ['DATA_PREFIX']
client = boto3.client('stepfunctions')

def handler(event, context):
   '''
   Launches interim step function from sns trigger
   
   :param event: - information regarding the output to pull from
   :param context:
   :return:
   '''
   client.start_execution(
      stateMachineArn=SF_ARN,
      input=json.dumps({"name": DATA_PREFIX})
)