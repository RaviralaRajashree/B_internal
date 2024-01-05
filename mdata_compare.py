import os
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
from dotenv import load_dotenv
from pathlib import Path
from deepdiff import DeepDiff

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
con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
#print("Postgresql connection established successfully")


table_name='config_table'
cursor = con.cursor()

"""folder_path = 'C:/Users/sumanth.gorantla/Desktop/sumanth_data/sample datasets'
# Get a list of all files in the specified folder
file_names = os.listdir(folder_path)
file_names_list=[]
# Print the file names
for file_name in file_names:
    file_names_list.append(file_name)
#print("...",file_names_list)

for file in file_names_list:
    folder_path = 'C:/Users/sumanth.gorantla/Desktop/sumanth_data/sample datasets'
    folder_path=folder_path+"\\"+file
    #print("file",folder_path)
    df = pd.read_csv(folder_path)
    metadata = df.dtypes.apply(lambda x: x.name).to_dict()
    metadata_json = []
    ft_map = []
    for k, v in metadata.items():
        file_schema = {
            "column_name": k, "data_type": v, "primary_key": False, "is_unique": False, "is_null": False
        }
        metadata_json.append(file_schema)
        mdata = json.dumps(metadata_json)
        #print(mdata)
        ft_json = {
            "f_column": k , "t_column": k
        }
        ft_map.append(ft_json)
        #print(".........",ft_map)
    #print(type(metadata_json))
    #Remove extension from file name
    fi=Path(file).stem
    #print(fi)
    current_date = date.today()
    # Insert values into the configure_Table   


    insert_query = f"INSERT INTO cardworks_internal.public.{table_name}(F_name, T_name, start_date,File_schema, Table_schema, File_table_mapping, Process_file,active_flag,update_type,track_changes) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)"
    cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} where f_name = %s);", (fi,))
    fi_exists = cursor.fetchone()[0]
    #print(fi_exists)
    if fi_exists:
        print(f"the file with the name {fi} already exists.....comparing metadata of both files")
        query=f"select file_schema from {table_name} where f_name like %s "
        cursor.execute(query,(fi+'%',))
        existing_file_schema = cursor.fetchall()
        #print(existing_file_schema[0][0])
        diff = DeepDiff(existing_file_schema[0][0], metadata_json, ignore_order=True)
        print(diff)
        if diff=={}:
            print("No changes in metadata")
        else:
            cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","changes"))
    else:
        cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","changes"))


con.commit()"""


def get_csv_file_names(folder_path):
    file_names = os.listdir(folder_path)
    file_names_list=[]
    for file_name in file_names:
        if file_name.endswith(".csv"):
            file_names_list.append(file_name)
    return file_names_list

def insert_data(file_names_list,folder_path):
    for file in file_names_list:
        file_path=folder_path+"\\"+file
        # print("file",file_path)
        df = pd.read_csv(file_path)
        metadata = df.dtypes.apply(lambda x: x.name).to_dict()
        metadata_json = []
        ft_map = []
        for k, v in metadata.items():
            file_schema = {
                "column_name": k, "data_type": v, "primary_key": False, "is_unique": False, "is_null": False
            }
            metadata_json.append(file_schema)
            mdata = json.dumps(metadata_json, indent=4)
            ft_json = {
            "f_column": k , "t_column": k
            }
            ft_map.append(ft_json)
            #print(".........",ft_map)

        #print(len(metadata_json[0]))
        for k in range(0,len(metadata_json)):
            column_name=metadata_json[k]["column_name"]
            #print(column_name)
            dtype=metadata_json[k]["data_type"]
            #print(dtype)
            if dtype=="object":
                metadata_json[k]["data_type"]="VARCHAR(255)"
                #dtype="VARCHAR(255)"
            elif dtype=="int64":
                metadata_json[k]["data_type"]="bigint"
                #dtype="bigint"
            elif dtype=="float64":
                metadata_json[k]["data_type"]="float"
                #dtype="float"
        print(metadata_json)
        #Remove extension from file name
        fi=Path(file).stem
        current_date = date.today()
        # Insert values into the configure_Table
        insert_query = f"INSERT INTO cardworks_internal.public.{table_name}(F_name, T_name, start_date, File_schema, Table_schema, File_table_mapping, Process_file,active_flag,update_type,track_changes) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)"
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} where f_name = %s);", (fi,))
        fi_exists = cursor.fetchone()[0]
        #print(fi_exists)
        if fi_exists:
            print(f"the file with the name {fi} already exists.....comparing metadata of both files")
            query=f"select file_schema from {table_name} where f_name like %s "
            cursor.execute(query,(fi+'%',))
            existing_file_schema = cursor.fetchall()
            #print(existing_file_schema[0][0])
            diff = DeepDiff(existing_file_schema[0][0], metadata_json, ignore_order=True)
            #print(diff)
            if diff=={}:
                print("No changes in metadata")
            else:
                cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual",diff))
        else:
            cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","changes"))

   
    print("Data inserted")

folder_path = os.getcwd()
 # folder_path = 'C:\\Users\\rajkumar.medishetti\\Desktop\\Card works\\test_files'
file_names_list=get_csv_file_names(folder_path)
#print("File names list ",file_names_list)
insert_data(file_names_list,folder_path)
con.commit()