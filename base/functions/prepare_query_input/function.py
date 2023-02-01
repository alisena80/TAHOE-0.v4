import boto3
import json
import hashlib
import os
from datetime import timedelta, datetime
from tahoelogger import log

import botocore
import io

s3 = boto3.resource("s3")
sf = boto3.client("stepfunctions")
OUTPUT_BUCKET = os.environ["output_bucket"]
cache_bucket = s3.Bucket(os.environ["output_bucket"]) 

logger = log.TahoeLogger().getStandardLogger()
def handler(event, context):
    '''
    Formats query list output to object event to make named consumable references

    :param event: - information regarding the output to pull from
    :param context:
    :return list_to_object: - Returns dictionary representation of list {Source:event["output"]["Output"]}
    '''
    print("Processing Event", event)
    # retrieval_time is when the query was innitiated and compared for caching
    retrieval_time = datetime.now()
    print(retrieval_time)
    for q in event["queries"]:
        # get query arn specfic to this datasource
        q["arn"] = os.environ[q["type"] + "_arn"]

        # hash query to create s3 location
        if "additional_details" not in q:
             q["additional_details"] = {}
        # Use md5 hash to add the ability to have a sub dataset of based on query. 
        hashed = hash_query(f"{json.dumps(q['query'])}".encode())

        q["additional_details"]["hash"] = hashed
        # ie cdm data is joined by ip to cyhy data. This cdm data is a random adhoc query. The combination and additiional transformation to this data is then uploaded to an s3 location as a file.
        # Default is not to persist the query
        if "persist" not in q["additional_details"]:
            q["additional_details"]["persist"] = False
        # Persisted queries are run on cron job vs normal queries which are api driven. 
        persisted =  q["additional_details"]["persist"]

        q["additional_details"]['timestamp_start'] = q["additional_details"]['timestamp_start'] if "timestamp_start" in q["additional_details"] else None
        q["additional_details"]['timestamp_end'] = q["additional_details"]['timestamp_start'] if "timestamp_end" in q["additional_details"] else None

        if not persisted:
          # check persisted and non persisted for cache
          q["additional_details"]["location"] = f"s3://{OUTPUT_BUCKET}/{q['type']}/{hashed}/"
          key = f"{q['type']}/{hashed}/"
          objs = list(cache_bucket.objects.filter(Prefix=key))

          q["additional_details"]["refetch"] = q["additional_details"]["refetch"] if "refetch" in q["additional_details"] else 3

          objs = sort_bucket_by_date(objs)
          # Data exists and it has at least one file 
          if len(objs) > 0:
            # If latest data has been stale for longer than x or 3 days cache is not valid and refetch with latest timestamp
            cache_diff = retrieval_time - timedelta(days=q["additional_details"]["refetch"])
            if objs[0].key != key and objs[0].last_modified.replace(tzinfo = None) > cache_diff.replace(microsecond=0):
              q["cache"] = "True"
            else:
              # Query required
              # q["additional_details"]['timestamp'] = str(objs[0].last_modified.replace(tzinfo = None))
              print("Requery All")
        
          # Assume persisted data is fresh
          persisted_key = f"{q['type']}/persisted/{q['query']['index']}/{hashed}/"
          objs = list(cache_bucket.objects.filter(Prefix=persisted_key))
          print(objs)
          if len(objs) > 0:
            q["cache"] = "True"
            # update location to point to persisted bucket over non persisted
            q["additional_details"]["location"] = f"s3://{OUTPUT_BUCKET}/{q['type']}/persisted/{q['query']['index']}/{hashed}/"
        else:
          # Create hash to query/name reference
          name = q["additional_details"]["name"] if "name" in q["additional_details"] else query_dump
          description = q["additional_details"]["description"] if "description" in  q["additional_details"] else "Default"

          # always requery if persisted
          q["additional_details"]["location"] = f"s3://{OUTPUT_BUCKET}/{q['type']}/persisted/{q['query']['index']}/{hashed}/"
          persisted_key = f"{q['type']}/persisted/{q['query']['index']}/{hashed}/"
          objs = list(cache_bucket.objects.filter(Prefix=persisted_key))
          objs = sort_bucket_by_date(objs)
          if len(objs) > 0:
            print("Requery all")
            # update retrival time once query is sucessful. Update name/descriptrion if default is used. TODO once proper datastore tahoe datastore location is decided.
            # update_to_index_file(q['type'], hashed, name, description, query_dump, retrieval_time)
            # time = str(objs[0].last_modified.replace(tzinfo = None))
            # q["additional_details"]['timestamp_start'] = time
            # q["additional_details"]['timestamp_end'] = retrieval_time
        
          query_dump = json.dumps(q["query"])
          # first pull
          try:
            add_to_index_file(q['type'], hashed, name, description, query_dump)
          except Exception as err:
            print('Handling run-time error:', err)


    return event

def add_to_index_file(type: str, hash: str, name: str, description: str, query: str):
    '''
    add index record to track datasets
    TODO Use datastore for tahoe system metadata
    :param type: table name
    :param hash: hash to add
    :param name: name to add
    :param description: description to add
    :param query: query that relates to this hash and name
    '''
    logger.dblog(log.IndexLog(data_prefix=type, name=name, description=description, hash=hash, query=query, index_location=f"s3://{OUTPUT_BUCKET}/{type}/persisted/index/index.json"))
    

def sort_bucket_by_date(objects):
  return sorted(objects, key=lambda x: x.last_modified.replace(tzinfo = None), reverse=True)

def hash_query(query):
    return hashlib.md5(query).hexdigest()