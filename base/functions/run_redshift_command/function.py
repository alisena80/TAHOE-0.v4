import os
import boto3
import botocore.session as bc
import json
import time

redshift_secret_name = os.environ["REDSHIFT_SECRET_NAME"]


def handler(event, context):
    query = event["sql_query"]

    redshift = setup_redshift()

    print("Executing Redshift SQL query: " + query)

    client_redshift = redshift["client"]
    results = client_redshift.execute_statement(Database="dev", SecretArn=redshift["secret_arn"], Sql=query,
                                      ClusterIdentifier=redshift["cluster_id"])

    results_id = results["Id"]

    results = client_redshift.describe_statement(Id=results_id)
    status = results["Status"]

    # Loop through the job's status until the job has either finished or failed
    in_progress_statuses = ["SUBMITTED", "PICKED", "STARTED"]
    while status in in_progress_statuses:
        time.sleep(0.1)
        results = client_redshift.describe_statement(Id=results_id)
        if results["Status"] != status:
            status = results["Status"]

    print("Query complete!")


def setup_redshift():
    """
    Creates a connection to redshift to use for the queries.
    Will persist until released by the lambda.
    :return: References to the redshift client, secret arn, and the cluster id
    """
    session = boto3.session.Session()
    region = session.region_name

    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=redshift_secret_name
    )

    secret_arn = get_secret_value_response['ARN']
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    cluster_id = secret_json['dbClusterIdentifier']

    bc_session = bc.get_session()

    session = boto3.Session(
        botocore_session=bc_session,
        region_name=region,
    )

    # Setup the client
    client_redshift = session.client("redshift-data")
    return {"client": client_redshift, "secret_arn": secret_arn, "cluster_id": cluster_id}
