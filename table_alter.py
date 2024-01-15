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
            # print(row)
            print(row['id'],":",row['track_changes'].keys())
            if 'iterable_item_added' in row['track_changes'].keys():
                # print(row['id'],":","columns added:",list(row['track_changes']['iterable_item_added'].values()))
                cols_added = list(row['track_changes']['iterable_item_added'].values())
                for col in cols_added:
                    table_cols_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}';"
                    cursor.execute(table_cols_query)
                    table_cols = cursor.fetchall()
                    table_cols = [item[0] for item in table_cols]
                    # print("TABLE COLUMNS:",table_cols,"-----",col['column_name'])
                    # print("****************",col['column_name'].lower() in table_cols)
                    if col['column_name'].lower() in table_cols:
                        col_name = f"{col['column_name']}_1"
                    else:
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
                    con.commit()
            if ('values_changed' in row['track_changes'].keys()) and ('iterable_item_added' not in row['track_changes'].keys()) and ('iterable_item_removed' not in row['track_changes'].keys()):
                cols_altered = list(row['track_changes']['values_changed'].keys())
                for col in cols_altered:
                    col_index = col.split(']')[0].split('[')[-1]
                    # print(row['File_schema'])
                    col_name = row['File_schema'][int(col_index)]['column_name']
                    new_dtype = row['track_changes']['values_changed'][col]['new_value']
                    # print("*****************",col_index,col_name,new_dtype)
                    add_col_query = f'ALTER TABLE {table_name} ADD {col_name}_1 {new_dtype}'
                    cursor.execute(add_col_query)
                    con.commit()
            if 'iterable_item_removed' in row['track_changes'].keys():
                # print(row['id'],":","columns removed:",row['track_changes']['iterable_item_removed'])
                pass

con.commit()