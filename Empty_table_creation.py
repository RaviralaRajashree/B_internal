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
table_name='config_table'
cursor= con.cursor()

query=f"select t_name from {table_name} where process_file='True'"
cursor.execute(query)
results = cursor.fetchall()
table_names = [value[0] for value in results]
#print("table names..",table_names)
for i in table_names:
    st=''
    q=f"select table_schema from {table_name} where t_name='{i}'"
    cursor.execute(q)
    result = cursor.fetchall()
    print(result)
    for j in result:
        for k in range(0,len(j[0])):
            column_name=j[0][k]["column_name"]
            dtype=j[0][k]["data_type"]
            if dtype=="object":
                dtype="VARCHAR(255)"
            elif dtype=="int64":
                dtype="bigint"
            elif dtype=="float64":
                dtype="float"
            primary_key=j[0][k]["primary_key"]
            if primary_key==True:
                primary_key="PRIMARY KEY"
            else:
                primary_key=""
            is_unique=j[0][k]["is_unique"]
            if is_unique==True:
                is_unique="UNIQUE"
            else:
                is_unique=""

            is_null=j[0][k]["is_null"]
            if is_null==True:
                is_null="NULL"
            else:
                is_null=""

            fields=f"{column_name} {dtype} {primary_key} {is_unique} {is_null}, "
            st=st+fields
    create_query=f"create table {i}("+st
    create_query=create_query[:-2]
    create_table_query=create_query+")"
    # print(create_table_query,"\n")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = %s);", (i,)) 
    table_exists = cursor.fetchone()[0]
    if table_exists:
        cursor.execute(f"drop table {i}")
        # print(f"table {i} dropped")
    # else:
    #     print(f"The '{i}' table is created")
    cursor.execute(create_table_query)
    #print(f"The '{i}' table is created")
con.commit()


