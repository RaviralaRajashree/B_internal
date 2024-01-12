import pandas as pd
import boto3
import psycopg2 as pg
import json
import os
from io import StringIO
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

postgresql_database = os.environ.get("POSTGRESQL_DATABASE")
postgresql_user = os.environ.get("POSTGRESQL_USER")
postgresql_password = os.environ.get("POSTGRESQL_PASSWORD")
postgresql_host = os.environ.get("POSTGRESQL_HOST")
postgresql_port = os.environ.get("POSTGRESQL_PORT")


 # Establish a connection to PostgreSQL
def pg_connect(database,user,password,host,port):
    connection = pg.connect(
        database = database,
        user = user,
        password = password,
        host = host,
        port = port
    )
    return connection


def create_table(table_name,cursor_obj):
    cursor_obj.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = %s);", (table_name,))
    table_exists = cursor_obj.fetchone()[0]
    if table_exists:
        print(f"The '{table_name}' already exists.")
        # If table exists truncate the table
        truncate_query=f"TRUNCATE TABLE {table_name}"
        print(f"The '{table_name}' truncated.")
        cursor_obj.execute(truncate_query)
    else:
        # Create table if table doesn't exist
        create_table_query = f"CREATE TABLE {table_name}(id serial NOT NULL PRIMARY KEY,F_name varchar(255),T_name varchar,File_schema json,Table_schema json,File_table_mapping json,Process_file boolean)"
        cursor_obj.execute(create_table_query)
        print(f"The {table_name} is created")

 # Initialize connection to AWS S3
def s3_connect():
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
                    aws_secret_access_key=os.environ.get("AWS_SECRET_KEY"), 
                    region_name=os.environ.get("AWS_REGION_NAME"))
    return s3

def insert_data(file_paths,bucket_name):
    for file_path in file_paths:
        cols = s3.get_object(Bucket=bucket_name, Key=file_path)
        first_few_lines = cols['Body'].read().decode('utf-8').split('\n')[:5]
        tables = pd.read_csv(StringIO('\n'.join(first_few_lines)))
        metadata = tables.dtypes.apply(lambda x: x.name).to_dict()
        
        # Convert metadata to a JSON
        metadata_json = []
        ft_map = []
        for k, v in metadata.items():
            file_schema = {
                "column_name": k, "data_type": v, "primary_key": False, "is_unique": False, "is_null": False
            }
            metadata_json.append(file_schema)
            mdata = json.dumps(metadata_json, indent=4)
        #print(mdata)
            ft_json = {
            "f_column": k , "t_column": k
            }
            ft_map.append(ft_json)
            #print(".........",ft_map)

        #Remove extension from file name
        li=file_path.split(".")

        # Insert values into the configure_Table
        insert_query = f"INSERT INTO cardworks_internal.public.{table_name}(F_name, T_name, File_schema, Table_schema, File_table_mapping, Process_file) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor_obj.execute(insert_query, (li[0], li[0],mdata, mdata, json.dumps(ft_map,indent=4), True))


con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name='config_table'
cursor_obj = con.cursor()
create_table(table_name,cursor_obj)

s3=s3_connect()
if s3:
    print("AWS S3 connection established successfully")
else:
    print("AWS S3 connection failed")

# List objects in the S3 bucket
bucket_name='cardworks-internal'
response = s3.list_objects_v2(Bucket=bucket_name)
file_paths = [obj['Key'] for obj in response.get('Contents', [])]
print(file_paths)
insert_data(file_paths,bucket_name) 
con.commit()

