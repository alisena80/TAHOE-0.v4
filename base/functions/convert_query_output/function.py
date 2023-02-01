import boto3
import json
import hashlib

def handler(event, context):
    '''
    Formats query list output to object event to make named consumable references

    :param event: - information regarding the output to pull from
    :param context:
    :return list_to_object: - Returns dictionary representation of list {Source:event["output"]["Output"]}
    '''
    print("Processing Event", event)
    list_to_object = {}
    for x in event["output"]:
        list_to_object[x["Source"]] = json.dumps(x)
    return list_to_object

