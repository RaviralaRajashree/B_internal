import pandas as pd
import os
import psycopg2 as pg
from dotenv import load_dotenv
from ydata_profiling import ProfileReport
import json
from pathlib import Path


###### connection for postgres

load_dotenv()

postgresql_database = os.environ.get("POSTGRESQL_DATABASE")
postgresql_user = os.environ.get("POSTGRESQL_USER")
postgresql_password = os.environ.get("POSTGRESQL_PASSWORD")
postgresql_host = os.environ.get("POSTGRESQL_HOST")
postgresql_port = os.environ.get("POSTGRESQL_PORT")

def pg_connect(database,host,user,password,port):
    connection = pg.connect(
        database=database,
        user=user,
        password=password,
        host = host,
        port=port
    )
    return connection

con = pg_connect(postgresql_database,postgresql_host,postgresql_user,postgresql_password,postgresql_port)
cursor = con.cursor()
print("postgres connection established successfully")


#######  below code checks for the stats table and creates one if not exists

def create_stats_table(stats_table):
    
    cursor.execute("select EXISTS(select 1 from pg_tables where tablename = %s)", (stats_table,))
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("config table already present")

    else:
        stats_table_query = """create table stats_table (id serial NOT NULL PRIMARY KEY, table_name varchar(255), column_name varchar(255), column_dtype varchar(255), table_row_count bigint,
                            n_null float, p_null float, n_unique float, p_unique float, min_value float, max_value float, avg_value float, n_negative float, p_negative float,
                            min_length float, max_length float,n_zeros float)"""
        cursor.execute(stats_table_query)
        print("stats_table created successfully")

stats_table = "stats_table"
create_stats_table(stats_table)



##### finds the list of csv  files present in the 
def get_csv_file_names(folder_path):
    file_names = os.listdir(folder_path)
    file_names_list=[]
    for file_name in file_names:
        if file_name.endswith(".csv"):
            file_names_list.append(file_name)
    return file_names_list


folder_path = os.getcwd()
files_list = get_csv_file_names(folder_path)
print(files_list)

######## below code reads file and generate stats from it

for file in files_list:
    df = pd.read_csv(file,header=0)
    t_name = Path(file).stem
    file_columns = df.columns
    reporter = ProfileReport(df)
    json_report = reporter.to_json()
    # with open("data_profiling_report.json", "w") as json_file:
    #     json_file.write(json_report)
    stats = json.loads(json_report)
    #print(type(stats))
    #print(stats["table"]["types"]["DateTime"])
    table_row_count = stats['table']['n']

    
    for column in file_columns:
        if stats['variables'][column]['type'] == 'DateTime' :
            #print(stats['variables'][column]['n_unique'])
            column_name =column
            column_dtype = stats['variables'][column]['type']
            n_unique = stats['variables'][column]['n_unique']
            p_unique = stats['variables'][column]['p_unique']
            n_null = stats['variables'][column]['n_missing']
            p_null = stats['variables'][column]['p_missing']
            insert_query = f"insert into {stats_table} (table_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query,(t_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count,))

        elif stats['variables'][column]['type'] == 'Numeric' :
            column_name =column
            column_dtype = stats['variables'][column]['type']
            n_null = stats['variables'][column]['n_missing']
            p_null = stats['variables'][column]['p_missing']
            n_unique = stats['variables'][column]['n_unique']
            p_unique = stats['variables'][column]['p_unique']
            min_value = stats['variables'][column]['min']
            max_value = stats['variables'][column]['max']
            avg_value = stats['variables'][column]['mean']
            n_negative = stats['variables'][column]['n_negative']
            p_negative = stats['variables'][column]['p_negative']
            n_zeros = stats['variables'][column]['n_zeros']
            insert_query = f"insert into {stats_table} (table_name,column_name,column_dtype,n_null,p_null,n_unique,p_unique,min_value,max_value,avg_value,n_negative,p_negative,n_zeros,table_row_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query,(t_name,column_name,column_dtype,n_null,p_null,n_unique,p_unique,min_value,max_value,avg_value,n_negative,p_negative,n_zeros,table_row_count,))
        
        elif stats['variables'][column]['type'] == 'Categorical' or stats['variables'][column]['type'] == 'Text' :
            column_name =column
            column_dtype = stats['variables'][column]['type']
            n_unique = stats['variables'][column]['n_unique']
            p_unique = stats['variables'][column]['p_unique']
            n_null = stats['variables'][column]['n_missing']
            p_null = stats['variables'][column]['p_missing']
            max_length = stats['variables'][column]['max_length']
            min_length = stats['variables'][column]['min_length']
            insert_query = f"insert into {stats_table} (table_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,max_length,min_length,table_row_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query, (t_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,max_length,min_length,table_row_count,))

        elif stats['variables'][column]['type'] == 'Boolean' :
            column_name =column
            column_dtype = stats['variables'][column]['type']
            n_unique = stats['variables'][column]['n_unique']
            p_unique = stats['variables'][column]['p_unique']
            n_null = stats['variables'][column]['n_missing']
            p_null = stats['variables'][column]['p_missing']
            insert_query = f"insert into {stats_table} (table_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query,(t_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count,))
        else:
            column_name =column
            column_dtype = stats['variables'][column]['type']
            n_unique = stats['variables'][column]['n_unique']
            p_unique = stats['variables'][column]['p_unique']
            n_null = stats['variables'][column]['n_missing']
            p_null = stats['variables'][column]['p_missing']
            insert_query = f"insert into {stats_table} (table_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query,(t_name,column_name,column_dtype,n_unique,p_unique,n_null,p_null,table_row_count,))




con.commit()
