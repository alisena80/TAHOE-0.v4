# IMPORTS START
from tahoecommon import commonhttp
from tahoecommon import commons3
import boto3
import json
import time

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from tahoecommon import commonutil
# IMPORTS END


# ARG START
# args = getResolvedOptions(sys.argv, ["DETAILS", "EXECUTION", "QUERY", "SHODAN_BASE_URL", "SHODAN_SECRET_KEY", "continuous-log-logGroup"])


# details = args["DETAILS"]
# execution = args["EXECUTION"]
# query = args["QUERY"]
# shodan_base_url = args["SHODAN_BASE_URL"]
# shodan_secret_key = args["SHODAN_SECRET_KEY"]
# continuous_log_loggroup = args["continuous_log_logGroup"]

query = {"index":"shodan","query":{"query":{"path":"host/8.8.8.8"}}}
shodan_base_url = "https://api.shodan.io/shodan/"
shodan_secret_key = "tahoegarg44ugw1_shodan"
continuous_log_loggroup = "/aws-glue/jobs/tahoegarg44ugw1_glue_shodan_load_queryEtlpyshellJobs"

# ARG END



# LOG START
currenttime = commonutil.getdaytime()
logger = log.TahoeLogger().getPyshellLogger(continuous_log_loggroup, currenttime)
# LOG END


# CODE START
def main():
    """
    Make Shodan (GET) REST API query and downloads resulting json to s3 bucket.

    Relevant parameter Details:
    QUERY - contains relevant parameter to query/filter on. 
    SHODAN_SECRET_KEY - Shodan API KEY
    SHODAN_BASE_URL - Base url for Shodan REST API requests.
    """
    # query_detail = json.loads(query)["query"]
    query_add_details = query["query"]["query"]
    path_query = query_add_details["path"]
    print(path_query)
    secret_obj = get_secret(shodan_secret_key)
    api_key = json.loads(secret_obj)['API_KEY']

    print("shodan secret", shodan_secret_key)
    print("shodan base url", shodan_base_url)

    try:
        # additional_details = json.loads(details)
        additional_details = {
                "persist": True,
                "name": "ip",
                "description": "ip ranges 8.8.8.8",
                "hash": "f62db0423a03e5f222b38d6670eb5c7c",
                "location": "s3://tahoegarg44ugw1-s3-interim/shodan/persisted/shodan/f62dre0423a03e5f222b38d6670eb5c7c/"
            }

    except json.JSONDecodeError:
        raise ValueError("Query failed to parse")

    location = additional_details["location"]
    s3_bucket = location.split("//")[1].split("/")[0]
    print("bucket", s3_bucket)

    url = shodan_base_url + path_query + "?key=" + api_key
    filename = 'response_' + str(time.time()) + ".json"
    local_path = '/tmp/' + filename
    commonhttp.httpdownload_stream(url, local_path)

    s3_path = location.split("//")[1].split("/", 1)[1] + filename
    print(s3_path)

    commons3.s3_upload(local_path, s3_bucket, s3_path)
    logger.notify(log.NotifyQueryLog("shodan", additional_details=additional_details, query=query))


def get_secret(secret_id):
    client = boto3.client("secretsmanager")
    secret_response = client.get_secret_value(SecretId=secret_id)
    if 'SecretString' in secret_response:
        secret = secret_response['SecretString']
    return secret


main()

# CODE END
