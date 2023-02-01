import awsgi
from flask import (
    Flask,
    jsonify,
    request,
)
import boto3
import botocore.session as bc
import os
import json
import time
from flasgger import APISpec, Swagger
from flask_cors import CORS
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from nvd import NvdSchema, NvdQueryBodySchema, NvdDescriptionQueryBodySchema, NvdDescriptionListSchema
from cyhy import CyhyHostScansSchema, CyhyOwnersQueryBodySchema, CyhyHostQueryBodySchema, CyhyPortScansSchema, CyhyPortQueryBodySchema, CyhyTicketsQueryBodySchema, CyhyTicketsSchema, CyhyVulnSchema, CyhyVulnQueryBodySchema
from resources import CloudformationResource
import datetime
import 
sf = boto3.client("stepfunctions")

app = Flask(__name__)
CORS(app)
spec = APISpec(
    title="TAHOE API",
    version="0.0.1",
    openapi_version="2.0.0",
    plugins=[FlaskPlugin(), MarshmallowPlugin()],
)
api_key_scheme = {"type": "apiKey", "in": "header", "name": "X-API-Key"}
spec.components.security_scheme("ApiKeyAuth", api_key_scheme)
template = spec.to_flasgger(
    app,
    definitions=[NvdSchema, NvdQueryBodySchema, NvdDescriptionListSchema, CyhyHostScansSchema,
                 CyhyOwnersQueryBodySchema, CyhyHostQueryBodySchema, CyhyPortScansSchema, CyhyPortQueryBodySchema,
                 CyhyTicketsSchema, CyhyTicketsQueryBodySchema, CyhyVulnSchema, CyhyVulnQueryBodySchema,
                 CloudformationResource],)
print(template)
swag = Swagger(app, template=template)

STACK_PREFIX = os.environ["STACK_PREFIX"]
NVD_SCHEMA = os.environ["NVD_SCHEMA"]
CYHY_SCHEMA = os.environ["CYHY_SCHEMA"]

DEFAULT_LIMIT = 100

TABLE_JOINS = {"tickets": ["snapshots", "events", "loc"],
               "vuln_scans": ["snapshots"],
               "host_scans": ["snapshots", "classes"],
               "port_scans": ["snapshots", "service_cpe"]}


@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Index of routes
    ---
    description: Index of routes
    definitions:
      Endpoints:
        type: array
        items:
            type: string
    responses:
        200:
            description: List of endpoints
            content:
                schema:
                    type: string
                    example: '/nvd/descriptions POST: <keyword> | Queries NVD descriptions with the given keyword'
    """
    endpoints = ["Endpoints:",
                 "/nvd/descriptions POST: <keyword> | Queries NVD descriptions with the given keyword",
                 "/cyhy/owners POST | Returns a list of all cyhy owners",
                 "/search_owners POST: <keyword> | Searches all cyhy owners with the given keyword filter",
                 "/cyhy/host POST: <owner> <limit> | Returns cyhy host information for the given owner",
                 "/cyhy/post POST: <owner> <limit> | Returns cyhy port information for the given owner",
                 "/cyhy/tickets POST: <owner> <limit> "
                 "| Returns cyhy ticket information for the given owner",
                 "/cyhy/vuln POST: <owner> <limit> "
                 "| Returns cyhy vulnerability information for the given owner"]
    return jsonify(status=200, message="\n".join(endpoints))


@app.route('/query_report', methods=["POST"])
def query():
    """
    Post queries to step functions
    ---
    description: Post queries to step functions
    responses:
        200:
            description: No Error
            content:
                schema:
                    type: string
                    example: 'NOT OK'
    """
    if request.method == 'POST':

        print(request.data)

        data = json.loads(request.data.decode())
        print(data)
        for request_data in data:
            for rq in request_data["queries"]:
                rq["arn"] = os.environ[rq["type"] + "_arn"]
            sf.start_execution(
                stateMachineArn=os.environ["query_arn"],
                input=json.dumps(request_data)
            )
    return jsonify(status=200, message='NOT OK')


@app.route("/nvd/descriptions", methods=["POST"])
def search_nvd_keyword():
    """
    Queries NVD records based off the keyword field in the request
    ---
    description:  Queries NVD records based off the keyword field in the request
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/NvdDescriptionQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/NvdDescriptionList'
    """
    request_data = json.loads(request.data.decode("utf-8"))
    keyword = request_data["keyword"]

    query = 'select "cve.description.description_data.val.value" from ' + \
            NVD_SCHEMA + '."' + STACK_PREFIX + 'nvd_cve_description_description_data" where ' \
                                               '"cve.description.description_data.val.value" like \'%' + keyword + '%\' limit 5;'

    query_result = execute_query(query)
    return jsonify(status=200, message=query_result)


@app.route("/nvd", methods=["POST"])
def search_nvd():
    """
    Queries NVD records by published date and attack vector
    ---
    description:  Queries NVD records by published date and attack vector
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/NvdQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/Nvd'        
    """
    request_data = json.loads(request.data.decode("utf-8"))

    filters = []

    for date in ["publisheddate"]:
        if date in request_data:
            filters.append(add_date_to_query("publisheddate", request_data["publisheddate"]))

    if "attack_vector" in request_data:
        filters.append(f'"impact.basemetricv3.cvssv3.attackvector" = \'{request_data["attack_vector"]}\'')

    query = build_query(NVD_SCHEMA, STACK_PREFIX + "nvd", filters, request_data, "cve.cve_data_meta.id")

    query_result = execute_query(query)
    return jsonify(status=200, message=query_result)


@app.route("/cyhy/owners", methods=["POST"])
def search_owners():
    """
    Gets a list of cyhy owners in the dataset filtered by the keyword in the request.
    ---
    description:  Gets a list of cyhy owners in the dataset filtered by the keyword in the request.
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyOwnersQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CyhyHostScans'
    """
    query = 'select distinct owner from ' + CYHY_SCHEMA + '."' + STACK_PREFIX + 'cyhy_host_scans"'

    request_data = json.loads(request.data.decode("utf-8"))
    if "keyword" in request_data:
        keyword = request_data["keyword"]
        query += ' where ' + CYHY_SCHEMA + '."' + STACK_PREFIX + 'cyhy_host_scans"' + ".owner like '%" \
                 + keyword + "%'"

    query += ";"

    query_result = execute_query(query)
    query_result["query"] = query
    return jsonify(status=200, message=query_result)


@app.route("/cyhy/host", methods=["POST"])
def search_cyhy_host_owner():
    """
    Queries cyhy host records by the owner and limit parameters in the request data
    ---
    description:  Queries cyhy host records by the owner and limit parameters in the request data
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyHostQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CyhyHostScans'
    """
    request_data = json.loads(request.data.decode("utf-8"))

    filters = []
    for date_field in ["time"]:
        if date_field in request_data:
            filters.append(add_date_to_query(date_field, request_data[date_field]))

    query_result = search_cyhy_owner("host_scans", request_data, filters)
    return jsonify(status=query_result["return_code"], message=query_result["results"])


@app.route("/cyhy/port", methods=["POST"])
def search_cyhy_port_owner():
    """
    Queries cyhy port records by the owner and limit parameters in the request data
    ---
    description:  Queries cyhy port records by the owner and limit parameters in the request data
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyPortQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CyhyPortScans'
    """
    print(f"Request data: {request.data}")
    request_data = json.loads(request.data.decode("utf-8"))

    filters = []
    for date_field in ["time"]:
        if date_field in request_data:
            filters.append(add_date_to_query(date_field, request_data[date_field]))

    query_result = search_cyhy_owner("port_scans", request_data, filters)
    return jsonify(status=query_result["return_code"], message=query_result["results"])


@app.route("/cyhy/tickets", methods=["POST"])
def search_cyhy_tickets_owner():
    """
    Queries cyhy ticket records by the owner and limit parameters in the request data
    ---
    description:  Queries cyhy ticket records by the owner and limit parameters in the request data
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyTicketsQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CyhyTickets'
    """
    request_data = json.loads(request.data.decode("utf-8"))

    filters = []

    # Add tickets-specific filter fields
    for date_field in ["last_change", "time_closed", "time_opened"]:
        if date_field in request_data:
            filters.append(add_date_to_query(date_field, request_data[date_field]))

    query_result = search_cyhy_owner("tickets", request_data, filters)
    return jsonify(status=query_result["return_code"], message=query_result["results"])


@app.route("/cyhy/vuln", methods=["POST"])
def search_cyhy_vuln_owner():
    """
    Queries cyhy vulnerability records by the owner and limit parameters in the request data
    ---
    description:  Queries cyhy vulnerability records by the owner and limit parameters in the request data
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyVulnQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CyhyVuln'
    """
    # query_start_time = time.perf_counter()

    request_data = json.loads(request.data.decode("utf-8"))

    filters = []
    for date_field in ["patch_publication_date", "plugin_publication_date", "plugin_modification_date",
                       "vuln_publication_date", "time"]:
        if date_field in request_data:
            filters.append(add_date_to_query(date_field, request_data[date_field]))

    query_result = search_cyhy_owner("vuln_scans", request_data, filters)

    # print("Total query time: " + str(time.perf_counter() - query_start_time))
    return jsonify(status=query_result["return_code"], message=query_result["results"])


@app.route("/resources", methods=["GET"])
def get_resources():
    """
    Resources deployed
    ---
    description: Resources deployed
    parameters: 
        - in: body
          name: body
          required: True
          schema:
                $ref: '#/definitions/CyhyVulnQueryBody'
    responses:
        200:
            description: Query Results
            schema:
                $ref: '#/definitions/CloudformationResource'
    """
    cloudformation = boto3.client("cloudformation")
    all_stacks = []
    token = None
    # Find relevant stacks to this deployment
    while True:
        args = {"StackStatusFilter": ['CREATE_COMPLETE']}
        if token is not None:
            args["NextToken"] = token
        stacks = cloudformation.list_stacks(
            **args)
        all_stacks.extend([stack["StackName"] for stack in stacks["StackSummaries"] if STACK_PREFIX in stack["StackName"]])
        token = all_stacks["NextToken"] if "NextToken" in all_stacks else None
        if token is None:
            break
    all_resources = []
    token = None
    # of relevant stacks list all resources
    for stack in all_stacks:
        while True:
            args = {"StackName": stack}
            if token is not None:
                args["NextToken"] = token
            resources = cloudformation.list_stack_resources(**args)
            all_resources.extend([{"name": resource["PhysicalResourceId"].split("/")[-1] if ":" not in resource["PhysicalResourceId"].split("/")[-1] else resource["PhysicalResourceId"].split(":")[-1], "type":resource["ResourceType"] , "id":resource["LogicalResourceId"], "last_update": resource["LastUpdatedTimestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")} for resource in resources["StackResourceSummaries"]])
            token = resources["NextToken"] if "NextToken" in all_stacks else None
            if token is None:
                break
    return {"return_code": 200, "results": all_resources}


def search_cyhy_owner(search_type, request_data, filters=[]):
    """
    Searches cyhy databases with the given owner
    :param search_type: Which cyhy database to search
    :param request_data: Data send by the user for this request. Used to build the query
    :param filters: Any non-generic filters already defined
    :return: Json result of records
    """

    if "owner" in request_data:
        owner = request_data["owner"]
        filters.append(f" owner='{owner}'")

    # Add the necessary table joins by using the field in the main table and the id of the referenced field
    for join in TABLE_JOINS[search_type]:
        filters.append(f'"{CYHY_SCHEMA}"."{STACK_PREFIX}cyhy_{search_type}"."{join.replace("_", ".")}" ='
                       f' "{CYHY_SCHEMA}"."{STACK_PREFIX}cyhy_{search_type}_{join}"."id"')

    tables = [f'{STACK_PREFIX}cyhy_{search_type}_{table}' for table in TABLE_JOINS[search_type]]
    tables = [f'{STACK_PREFIX}cyhy_{search_type}'] + tables

    query = build_query(CYHY_SCHEMA, tables, filters, request_data, "_id.oid")

    try:
        results = execute_query(query)
        return {"return_code": 200, "results": results}
    except Exception as error:
        return {"return_code": 500, "results": str(error)}


def build_query(schema, tables, filters, request_data, default_sort_column):
    """
    Builds a generic sql query with the given information
    :param schema: Schema to query against
    :param tables: Table in the schema to query
    :param filters: Existing query-specific filters to add to the query. Will be used to make a where query
        joined by "and"
    :param request_data: User data to parse for query information
    :param default_sort_column: Column to sort by if the user did not specify
    :return: select query ready to send to redshift
    """
    # If a single table was specified instead of an array, add it as a single-entry array
    if isinstance(tables, str):
        tables = [tables]

    if "count" in request_data and request_data["count"]:
        return build_count_query(schema, tables, filters)
    return build_record_query(schema, tables, filters, request_data, default_sort_column)


def build_count_query(schema, tables, filters):
    """
    Builds a count query with the given filters
    :param schema: Schema to query against
    :param tables: Table in the schema to query
    :param filters: Existing query-specific filters to add to the query. Will be used to make a where query
        joined by "and"
    :return: count query ready to send to redshift
    """
    # Append the schema name to all the table names
    normalized_tables = [f'"{schema}"."{table}"' for table in tables]
    query = f'select * from {", ".join(normalized_tables)}'

    if len(filters) > 0:
        query += " where "
        query += " and ".join(filters)

    # Filter out any unsafe ; in the query
    query = query.replace(";", "")

    query += ";"
    print(f"Count query: {query}")

    return query


def build_record_query(schema, tables, filters, request_data, default_sort_column):
    """
    Builds a generic record search query with the given information
    :param schema: Schema to query against
    :param tables: Table(s) in the schema to query
    :param filters: Existing query-specific filters to add to the query. Will be used to make a where query
        joined by "and"
    :param request_data: User data to parse for query information
    :param default_sort_column: Column to sort by if the user did not specify
    :return: select query ready to send to redshift
    """
    # Append the schema name to all the table names
    normalized_tables = [f'"{schema}"."{table}"' for table in tables]
    query = f'select * from {", ".join(normalized_tables)}'

    # Add all the filters into the where query
    if len(filters) > 0:
        query += " where "
        query += " and ".join(filters)

    # Add the sorting. Leaving this out leads to some inconsistent results on repeat queries
    sort = request_data["sort"] if "sort" in request_data else default_sort_column
    sort_dir = request_data["sort_dir"] if "sort_dir" in request_data else "desc"
    query += f' order by "{sort}" {sort_dir}'

    limit = request_data["limit"] if "limit" in request_data else DEFAULT_LIMIT
    query += f" limit {limit}"

    if "page" in request_data:
        # Offset is the amount of records you want to skip, so calculate based off the page
        # using int() just in case the user gave us a string
        offset = (int(request_data["page"]) - 1) * limit
        query += f' offset {offset}'

    # Filter out any unsafe ; in the query
    query = query.replace(";", "")

    # Close out the query
    query += ";"
    print(f"Query: {query}")

    return query


def setup_redshift():
    """
    Creates a connection to redshift to use for the queries.
    Will persist until released by the lambda.
    :return: References to the redshift client, secret arn, and the cluster id
    """
    redshift_secret_name = "redshiftSecret0989615F-P9O4JpKGq6SG"
    session = boto3.session.Session()
    region = session.region_name

    if "gov" in region.lower():
        redshift_secret_name = "redshiftSecret0989615F-TtjjY9SI4bYs"

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


def add_date_to_query(field, value):
    range = value.split(" TO ")
    if len(range) > 1:
        return f"{field} between '{range[0]}' and '{range[1]}'"


def execute_query(query: str):
    """
    Execute the given SQL query and return the results
    :param query: SQL query to run
    :return: Results parsed and cleaned
    """
    client_redshift = redshift["client"]

    # Execute statement will return an id that points to the results instead of the results themselves
    try:
        res = client_redshift.execute_statement(Database='dev', SecretArn=redshift["secret_arn"], Sql=query,
                                                ClusterIdentifier=redshift["cluster_id"])
    except Exception as error:
        if "PAUSED" in str(error):
            return {"Records": "ERROR: Redshift is currently PAUSED. Please verify with your administrator "
                               "that Redshift is running and try again."}

    results_id = res["Id"]

    # Use the results id to get the actual results
    results = client_redshift.describe_statement(Id=results_id)
    status = results["Status"]

    # Loop through the job's status until the job has either finished or failed
    in_progress_statuses = ["SUBMITTED", "PICKED", "STARTED"]
    while status in in_progress_statuses:
        time.sleep(0.1)
        results = client_redshift.describe_statement(Id=results_id)
        if results["Status"] != status:
            status = results["Status"]

    query_result = client_redshift.get_statement_result(Id=results_id)

    # Results are separate from the column metadata, so combine them first
    parsed_records = []
    column_names = query_result["ColumnMetadata"]

    for record in query_result["Records"]:
        new_record = []
        for x, field in enumerate(record):
            # Each record entry is a dict with a single value, so use the only value present
            new_record.append({column_names[x]["name"]: list(field.values())[0]})
        parsed_records.append(new_record)

    return {"records": parsed_records, "query": query}


@app.route("/openapi", methods=["GET"])
def openapi():
    global swag
    if swag == None:
        return jsonify(status=500, message="Swagger not initialized")
    return jsonify(swag.get_apispecs())


def handler(event, context):
    if event is not None and event['headers'] is not None:
        swag.config['host'] = event['headers']['Host']
        swag.config['schemes'] = [event['headers']['X-Forwarded-Proto']]
    return awsgi.response(app, event, context, base64_content_types={"image/png"})


redshift = setup_redshift()
