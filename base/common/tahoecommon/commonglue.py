import boto3
import sys
import subprocess
from typing import List

"""
Provides boto3 Glue apis and glue related helper functions
"""


def get_tables_names(database: str) -> dict:
    """
    Get all tables in database.

    :param database: Glue database name
    :returns: Glue get tables api result or None if call fails
    """
    client = boto3.client('glue')
    try:
        tables = list(map(lambda x: x["Name"], client.get_tables(DatabaseName=database)['TableList']))
    except:
        print("FAILED TO FIND DATABASE IN CATALOUGE")
        tables = None
    return tables


def get_table(database: str, table: str) -> dict:
    """
    Glue get table.

    :param database: Glue database name
    :param table: Glue table name

    :returns: Glue get table api or None if call fails
    """
    client = boto3.client('glue')
    try:
        table_info = client.get_table(DatabaseName=database, Name=table)
    except:
        print("FAILED TO FIND TABLE IN CATALOUGE")
        table_info = None
    return table_info


def write_relationalized_frame_to_output(glue_context, output_location: str, frame):
    """
    Write the relationalized frame to s3 in glue parquet format.

    :param glue_context: glue context to use when writing
    :param output_location: S3 bucket url to write to
    :param frame: DynamicFrameCollection to write
    """
    for df_name in frame.keys():
        m_df = frame.select(df_name)
        print("Writing to Parquet File: ", df_name)
        glue_context.write_dynamic_frame.from_options(frame=m_df,
                                                      connection_type="s3",
                                                      connection_options={"path": output_location + "/" + df_name},
                                                      format="glueparquet", format_options={"compression": "snappy"})


def install_packages(modules: List[str], host=None):
    call = [sys.executable, "-m", "pip", "install"]
    if host:
        call.extend(host.split(" "))
    call.extend(["-t", ".", module])
    modules = modules.split(',')
    for module in modules:
        subprocess.check_call(call)
