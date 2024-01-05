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
        # If table exists truncate the table
        #truncate_query=f"TRUNCATE TABLE {table_name}"
        #print(f"The '{table_name}' truncated.")
        #cursor_obj.execute(truncate_query)
    else:
        # Create table if table doesn't exist
        create_table_query = f"CREATE TABLE {table_name}(id serial NOT NULL PRIMARY KEY,F_name varchar(255), T_name varchar, start_date date, expiry_date date,File_schema json,Table_schema json,File_table_mapping json,Process_file boolean,active_flag boolean, update_type VARCHAR(255), track_changes varchar(225))"
        cursor_obj.execute(create_table_query)
        print(f"The {table_name} is created")

# def get_csv_file_names(folder_path):
#     file_names = os.listdir(folder_path)
#     file_names_list=[]
#     for file_name in file_names:
#         if file_name.endswith(".csv"):
#             file_names_list.append(file_name)
#     return file_names_list

# def insert_data(file_names_list,folder_path):
#     for file in file_names_list:
#         file_path=folder_path+"\\"+file
#         # print("file",file_path)
#         df = pd.read_csv(file_path)
#         metadata = df.dtypes.apply(lambda x: x.name).to_dict()
#         metadata_json = []
#         ft_map = []
#         for k, v in metadata.items():
#             file_schema = {
#                 "column_name": k, "data_type": v, "primary_key": False, "is_unique": False, "is_null": False
#             }
#             metadata_json.append(file_schema)
#             mdata = json.dumps(metadata_json, indent=4)
#             ft_json = {
#             "f_column": k , "t_column": k
#             }
#             ft_map.append(ft_json)
#             #print(".........",ft_map)

#         #Remove extension from file name
#         li=file.split(".")
#         current_date = date.today()
#         # Insert values into the configure_Table
#         insert_query = f"INSERT INTO cardworks_internal.public.{table_name}(F_name, T_name, start_date, File_schema, Table_schema, File_table_mapping, Process_file,active_flag,update_type,track_changes) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)"
#         cursor.execute(insert_query, (li[0], li[0],current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","changes"))   
#     print("Data inserted")

con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name='config_table'
cursor = con.cursor()
create_table(table_name,cursor)
#folder_path = os.getcwd()
# # folder_path = 'C:\\Users\\rajkumar.medishetti\\Desktop\\Card works\\test_files'
# file_names_list=get_csv_file_names(folder_path)
# print("File names list ",file_names_list)
# insert_data(file_names_list,folder_path)
con.commit()



