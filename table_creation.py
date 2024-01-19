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

con = pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name = 'config_table'
cursor = con.cursor()

query = f"select t_name from {table_name} where process_flag ='True' and active_flag = 'True'"
cursor.execute(query)
results = cursor.fetchall()
table_names = [value[0] for value in results]
print("table names..",table_names)
for table in table_names:
    st = ''
    table_Sch = f"select table_schema from {table_name} where t_name='{table}'"
    cursor.execute(table_Sch)
    result = cursor.fetchall()
    tableschema = result[0][0] 
    for tab_Sch in tableschema:
        column_name = tab_Sch["column_name"]
        dtype = tab_Sch["data_type"]
        primary_key = tab_Sch["primary_key"]
        if primary_key == True:
            primary_key = "PRIMARY KEY"
        else:
            primary_key = ""
        is_unique = tab_Sch["is_unique"]
        if is_unique == True:
            is_unique = "UNIQUE"
        else:
            is_unique = ""

        is_null = tab_Sch["is_null"]
        if is_null == True:
            is_null = "NULL"
        else:
            is_null = ""

        fields = f"{column_name} {dtype} {primary_key} {is_unique} {is_null}, "
        st = st+fields
    create_query = f"create table {table}("+st
    create_query = create_query[:-2]
    create_table_query = create_query+")"
    # print(create_table_query,"\n")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = %s);", (table.lower(),)) 
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute(create_table_query)
        print(f"The '{table}' table is created")
con.commit()
