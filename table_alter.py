import json
import psycopg2 as pg
import os
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

con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
config_table_name='config_table'
cursor= con.cursor()

select_query = f"SELECT * FROM {config_table_name};"
cursor.execute(select_query)
config_table = cursor.fetchall()
columns = ['id','F_name', 'T_name', 'start_date', 'expiry_date','File_schema','Table_schema','File_table_mapping','Process_flag','active_flag', 'update_type', 'track_changes','alter_table_flag']
config_table = [dict(zip(columns, values)) for values in config_table]
for row in config_table:
    table_name = row['T_name']
    if row['alter_table_flag']:
        if row['track_changes'] :
            print(row['id'],":",row['track_changes'])
            for change in row['track_changes']:
                if 'column_added' in  change.keys():
                    for col in change['column_added']:
                        col_name = col['column_name']
                        data_type = col['data_type']
                        if col['primary_key'] == True:
                            primary_key = "PRIMARY KEY"
                        else:
                            primary_key = ""
                        if col['is_unique'] == True:
                            is_unique = "UNIQUE"
                        else:
                            is_unique = ""
                        if col['is_null'] == True:
                            is_null="NULL"
                        else:
                            is_null=""
                        add_col_query = f'ALTER TABLE {table_name} ADD {col_name} {data_type} {primary_key} {is_unique} {is_null}'
                        cursor.execute(add_col_query)
                
                if 'column_changed' in  change.keys():
                    for col in change['column_changed']:
                        col_name = col['column_name'] + '_1'
                        if col['value_changed'] == 'data_type':
                            new_val = col['new_value']
                        add_col_query = f'ALTER TABLE {table_name} ADD {col_name} {new_val}'
                        cursor.execute(add_col_query)
                        # UPDATE table schema and file table mapping
                        for col1 in row['Table_schema']:
                            if col1['column_name'] == col['column_name']:
                                col1['column_name'] = col_name
                        for col2 in row['File_table_mapping']:
                            if col2['t_column'] == col['column_name']:
                                col2['t_column'] = col_name
                        
                        update_table_mapping_query = f"""UPDATE {config_table_name} SET Table_schema = '{json.dumps(row["Table_schema"])}', File_table_mapping = '{json.dumps(row["File_table_mapping"])}' WHERE f_name = '{table_name}' and id = {row['id']}"""
                        cursor.execute(update_table_mapping_query)


                if 'column_deleted' in  change.keys():
                    for col in change['column_deleted']:
                        print("*",col)
                    
                
                


con.commit()