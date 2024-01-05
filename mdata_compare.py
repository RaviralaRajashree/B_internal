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


#returns files list in local folder
def get_csv_file_names(folder_path):
    file_names = os.listdir(folder_path)
    file_names_list=[]
    for file_name in file_names:
        if file_name.endswith(".csv"):
            file_names_list.append(file_name)
    return file_names_list

#inserts row in config_table when new file is detected
def insert_data(file_names_list,folder_path):
    for file in file_names_list:
        file_path=folder_path+"\\"+file
        # print("file",file_path)
        df = pd.read_csv(file_path)
        metadata = df.dtypes.apply(lambda x: x.name).to_dict()
        metadata_json = []
        ft_map = []
        for k, v in metadata.items():
            file_schema = {"column_name": k, "data_type": v, "primary_key": False, "is_unique": False, "is_null": False}
            metadata_json.append(file_schema)
            ft_json = {"f_column": k , "t_column": k}
            ft_map.append(ft_json)

        for k in range(0,len(metadata_json)):
            column_name=metadata_json[k]["column_name"]
            dtype=metadata_json[k]["data_type"]
            if dtype=="object":
                metadata_json[k]["data_type"]="VARCHAR(255)"
            elif dtype=="int64":
                metadata_json[k]["data_type"]="bigint"
            elif dtype=="float64":
                metadata_json[k]["data_type"]="float"
        mdata = json.dumps(metadata_json, indent=4)
        print(metadata_json)
        #Remove extension from file name
        fi=Path(file).stem
        current_date = date.today()
        # Insert values into the configure_Table
        insert_query = f"INSERT INTO cardworks_internal.public.{table_name}(F_name, T_name, start_date,File_schema, Table_schema, File_table_mapping, Process_flag, active_flag, update_type,track_changes) VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"
        # update_query = f"UPDATE cardworks_internal.public.{table_name} SET expiry_date = {} WHERE f_name = {fi}"
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} where f_name = %s);", (fi,))
        fi_exists = cursor.fetchone()[0]
        if fi_exists:
            print(f"the file with the name {fi} already exists.....comparing metadata of both files")
            query=f"select file_schema from {table_name} where f_name like %s "
            cursor.execute(query,(fi+'%',))
            existing_file_schema = cursor.fetchall()
            diff = DeepDiff(existing_file_schema[0][0], metadata_json, ignore_order=True)
            dif=json.dumps(diff)
            if diff=={}:
                print("No changes in metadata")
                cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","No changes"))
            else:
                cursor.execute(insert_query, (fi, fi,current_date, mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual",dif))
        else:
            cursor.execute(insert_query, (fi, fi,current_date,mdata, mdata, json.dumps(ft_map,indent=4), True, True,"Manual","changes"))

   
    print("Data inserted")

folder_path = os.getcwd()
file_names_list = get_csv_file_names(folder_path)
table_name ='config_table'
cursor = con.cursor()
insert_data(file_names_list,folder_path)
con.commit()