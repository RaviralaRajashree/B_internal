import os
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
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
    else:
        # Create table if table doesn't exist
        create_table_query = f"CREATE TABLE {table_name}(id serial NOT NULL PRIMARY KEY,F_name varchar(255), T_name varchar, start_date date, expiry_date date,File_schema json,Table_schema json,File_table_mapping json,Process_flag boolean,active_flag boolean, update_type VARCHAR(255), track_changes json,alter_table_flag boolean)"
        cursor_obj.execute(create_table_query)
        print(f"The {table_name} is created")


con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name='config_table'
cursor = con.cursor()
create_table(table_name,cursor)
con.commit()



