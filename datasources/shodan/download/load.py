# IMPORTS START
from tahoecommon import commonhttp
from tahoecommon import commons3
import boto3
import json
import time
from string import punctuation

from awsglue.utils import getResolvedOptions
from tahoelogger import log
import sys

from tahoecommon import commonutil
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, ["DETAILS", "EXECUTION", "QUERY", "SHODAN_BASE_URL", "SHODAN_SECRET_KEY", "continuous-log-logGroup"])

details = args["DETAILS"]
execution = args["EXECUTION"]
query = args["QUERY"]
shodan_base_url = args["SHODAN_BASE_URL"]
shodan_secret_key = args["SHODAN_SECRET_KEY"]
continuous_log_loggroup = args["continuous_log_logGroup"]
# ARG END




# LOG START
currenttime = commonutil.getdaytime()
logger = log.TahoeLogger().getPyshellLogger(continuous_log_loggroup, currenttime)
# LOG END


# CODE START
def main():
    """
    Makes Shodan (GET) REST API query and downloads resulting json to s3 bucket.
    Relevant parameter Details:
    QUERY - contains relevant parameter to query/filter on. 
    SHODAN_SECRET_KEY - Shodan API KEY
    SHODAN_BASE_URL - Base url for Shodan REST API requests.
    """
    query_detail = json.loads(query)["query"]
    print(query_detail)
    query_add_details = query_detail["query"]
    path_query = query_add_details["path"]
    print(path_query)
    secret_obj = get_secret(shodan_secret_key)
    api_key = json.loads(secret_obj)['API_KEY']

    print("shodan secret", shodan_secret_key)
    print("shodan base url", shodan_base_url)

    try:
        print(details)
        additional_details = json.loads(details)
    except json.JSONDecodeError:
        raise ValueError("Query failed to parse")

    location = additional_details["location"]
    s3_bucket = location.split("//")[1].split("/")[0]
    print("bucket", s3_bucket)

    url = shodan_base_url + path_query + "?key=" + api_key
    filename = 'response_' + str(time.time()) + ".json"
    local_path = '/tmp/' + filename
    commonhttp.httpdownload_stream(url, local_path)

    output = None
    # check for case where no information for the query is returned from shodan
    with open(local_path, "r") as file:
        try:
            jsonData = json.load(file)
        except json.JSONDecodeError:
            raise ValueError("Results failed to parse")
        if "error" in jsonData:
            raise ValueError(f"No data found for query {query}. \r\n Shodan returned: {jsonData['error']}")
        output = pre_process_cols(jsonData)

    m_filename = 'response_' + str(time.time()) + "_modified.json"
    local_path = '/tmp/' + m_filename
    if output:
        with open(local_path, "w") as file:
            json.dump(output, file)
    else:
        raise ValueError(f"Preprocessing failed")

    s3_path = location.split("//")[1].split("/", 1)[1] + filename
    print(s3_path)

    commons3.s3_upload(local_path, s3_bucket, s3_path)
    logger.notify(log.NotifyQueryLog("shodan", additional_details=additional_details, query=query_detail))


def pre_process_cols(json_results: dict):
    keys = list(json_results.keys())
    for key in keys:
        print("key hit:", key)
        if isinstance(json_results[key], dict):
            json_results[key] = pre_process_cols(json_results[key])
        elif isinstance(json_results[key], list):
            if len(json_results[key]) > 0:
                if isinstance(json_results[key][0], dict):
                    for index, key1 in enumerate(json_results[key]):
                        json_results[key][index] = pre_process_cols(key1)
        modified_key = key

        # truncate to the only 255 characters
        if len(key) > 255:
            modified_key = modified_key[0:255]

        # remove/replace special characters
        for char in punctuation:
            modified_key = modified_key.replace(char, "_")

        # set to lower
        modified_key = modified_key.replace(" ", "_").lower()

        # only if key has been modified
        if modified_key != key:
            # check if the modified key already exists in the json
            if modified_key in json_results:
                raise ValueError(f"Duplicate json elements would result. Modified Key: {modified_key}, Key: {key}")

            # replace the key with the pre-processed / modified key
            json_results[modified_key] = json_results.pop(key)
    return json_results


def get_secret(secret_id):
    client = boto3.client("secretsmanager")
    secret_response = client.get_secret_value(SecretId=secret_id)
    if 'SecretString' in secret_response:
        secret = secret_response['SecretString']
    return secret


main()

# CODE END
