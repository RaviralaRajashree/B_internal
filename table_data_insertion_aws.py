import os
from io import StringIO
from dotenv import load_dotenv
import psycopg2 as pg
import boto3
import pandas as pd
from datetime import datetime
import math
import numpy as np
load_dotenv()

aws_access_key = os.environ.get("AWS_ACCESS_KEY")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
bucket_name = 'cardworks-internal'

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
print("Postgresql connection established successfully")
cursor= con.cursor()

s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
table_name='config_table'

select_table_query=f"select f_name from {table_name} where process_file='True'"
cursor.execute(select_table_query)
results = cursor.fetchall()
file_names = [value[0] for value in results]
print("file names..",file_names)


select_table_query=f"select t_name from {table_name} where process_file='True'"
cursor.execute(select_table_query)
results = cursor.fetchall()
table_names = [value[0] for value in results]
print("table names..",table_names)

response = s3.list_objects_v2(Bucket=bucket_name)
file_paths = [obj['Key'] for obj in response.get('Contents', [])]
print(file_paths)
for file in file_paths:
    for table in file_names:
        if f"{file}"[:-4]==f"{table}":
            column_names=""
            column=[]

            get_table_name_query=f"select t_name from {table_name} where f_name ='{table}'"
            cursor.execute(get_table_name_query)
            res = cursor.fetchall()
            for r in res:
                table=str(r)[2:-3]

            query = f"select file_table_mapping from {table_name} where t_name='{table}'"
            # print(query)
            cursor.execute(query)
            result = cursor.fetchall()
            for j in result:
                for k in range(0,len(j[0])):
                    column.append(j[0][k]["t_column"])
            for z in range(0,len(column)):
                column_names=column_names+column[z]+","
            column_names=column_names[:-1]
            # print(column_names)
            
            current_time = datetime.now().strftime("%H:%M:%S")

            csv_file = s3.get_object(Bucket=bucket_name, Key=f"{file}")
            csv_content = csv_file['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            print(f"{table}",df)
            data = [tuple(row) for row in df.values]
            # print(data)

            def replace_nan_with_none(t):
                return tuple(None if isinstance(value, float) and np.isnan(value) else value for value in t)
            processed_data = [replace_nan_with_none(row) for row in data]
            
            updated_table_name = f'{table}'

            values_placeholder = ', '.join(['%s' for _ in range(len(df.columns))])
            insert_query = f"INSERT INTO {updated_table_name} ({column_names}) VALUES ({values_placeholder})"
            cursor.executemany(insert_query, data)
            print(f"{table} Data inserted")
            
            new_time = datetime.now().strftime("%H:%M:%S")
            current_time_dt = datetime.strptime(current_time, "%H:%M:%S")
            new_current_time_dt = datetime.strptime(new_time, "%H:%M:%S")
            time_difference = new_current_time_dt - current_time_dt
            # print("Time Difference:..", time_difference)

con.commit()


