# get records with update_flag = True and previous record
# Compare tschema with fileschema or with previous tschema? 
# -- if compared with previous one new columns added will also reflect in update_details which should be reflected in diff column
# -- if compared with fileschema previous manual changes will also reflect in update_details where only last made changes should reflect
# OR COMPARE WITH FILESCHEMA GET UPDATE_DETAILS OF PREVIOUS RECORD, DIFF AND ELEMINATE THOSE CHANGES
# insert update_details

import json
import psycopg2 as pg
import os
from dotenv import load_dotenv
from json_compare import compare 

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

connection = pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name = 'config_table'
cursor = connection.cursor()
select_query = f"SELECT * FROM {table_name} WHERE update_flag = 'True' AND active_flag = 'True';"
cursor.execute(select_query)
update_rows = cursor.fetchall()
columns = ['id','F_name', 'T_name', 'start_date', 'expiry_date','File_schema','Table_schema','File_table_mapping','Process_flag','active_flag', 'update_type', 'track_changes','alter_table_flag','update_flag', 'update_details']
update_rows = [dict(zip(columns, values)) for values in update_rows]
for row in update_rows:
    differences = compare(row['File_schema'],row['Table_schema'])
    select_update_details = f"SELECT id, F_name, File_schema, Table_schema, update_flag, update_details FROM {table_name} WHERE F_name = {row['F_name']} ORDER BY id DESC LIMIT 2;"
    cursor.execute(select_update_details)
    cursor.fetchone()
    prev_row = cursor.fetchone()
    print(prev_row['update_details'])
    if 'column_added' in differences.keys():
        for diff in differences['column_added']:
            if diff in prev_row:
                differences['column_added'].remove(diff)
    if 'column_deleted' in differences.keys():
        for diff in differences['column_deleted']:
            if diff in prev_row:
                differences['column_deleted'].remove(diff)
    if 'column_changed' in differences.keys():
        for diff in differences['column_changed']:
            if diff in prev_row:
                differences['column_changed'].remove(diff)
    update_query = f"UPDATE {table_name} SET update_details = {json.dumps(differences)} WHERE id = {row['id']}"
    cursor.execute(update_query)
connection.commit()