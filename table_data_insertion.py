import os
# from io import StringIO
from dotenv import load_dotenv
import psycopg2 as pg
import pandas as pd
from datetime import datetime
import numpy as np
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
cursor = connection.cursor()

table_name = 'config_table'
# folder_path = os.getcwd()
folder_path = "../"

select_table_query = f"select f_name from {table_name} where process_flag = 'True';"
cursor.execute(select_table_query)
results = cursor.fetchall()
file_names = [value[0] for value in results]
print("file names to be processed:",file_names)

def get_csv_file_names(folder_path):
    file_names = os.listdir(folder_path)
    file_names_list=[]
    for file_name in file_names:
        if file_name.endswith(".csv"):
            file_names_list.append(file_name)
    return file_names_list


file_names_list=get_csv_file_names(folder_path)
print("Files in local folder ",file_names_list)   

select_table_query=f"select t_name from {table_name} where process_flag='True'"
cursor.execute(select_table_query)
results = cursor.fetchall()
table_names = [value[0] for value in results]
print("table names..",table_names)
  
for file in file_names_list:
    for table in file_names:
        if f"{file}"[:-4]==f"{table}":
            column_names=""
            column=[]
            get_table_name_query=f"select t_name from {table_name} where f_name ='{table}'"
            cursor.execute(get_table_name_query)
            res = cursor.fetchall()
            for r in res:
                table=str(r)[2:-3]

            query = f"select file_table_mapping from {table_name} where t_name='{table}' ORDER BY id DESC LIMIT 1;"
            cursor.execute(query)
            result = cursor.fetchall()
            # print("*************",result)
            for j in result:
                for k in range(0,len(j[0])):
                    column.append(j[0][k]["t_column"])
            for z in range(0,len(column)):
                column_names=column_names+column[z]+","
            column_names=column_names[:-1]
            # print(column_names)
            
            current_time = datetime.now().strftime("%H:%M:%S")

            file_path=folder_path+"\\"+file
            df = pd.read_csv(file_path)
            # print(f"{table}",df)

            data = [tuple(row) for row in df.values]
            # print("DATA:",data)
            
            updated_table_name = f'{table}'

            values_placeholder = ', '.join(['%s' for _ in range(len(df.columns))])
            insert_query = f"INSERT INTO {updated_table_name} ({column_names}) VALUES ({values_placeholder})"
            cursor.executemany(insert_query, data)
            print(f"{table} Data inserted")
            
            new_time = datetime.now().strftime("%H:%M:%S")
            current_time_dt = datetime.strptime(current_time, "%H:%M:%S")
            new_current_time_dt = datetime.strptime(new_time, "%H:%M:%S")
            time_difference = new_current_time_dt - current_time_dt
            print("Time Difference:..", time_difference)

connection.commit()
