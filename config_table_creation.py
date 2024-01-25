import os
import psycopg2 as pg
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
    else:
        # Create table if table doesn't exist
        create_table_query = f"CREATE TABLE {table_name}(id serial NOT NULL PRIMARY KEY,F_name varchar(255), T_name varchar, start_date date, expiry_date date,File_schema json,Table_schema json,File_table_mapping json,Process_flag boolean,active_flag boolean, update_type VARCHAR(255), track_changes json,alter_table_flag boolean,update_flag boolean, update_details json)"
        cursor_obj.execute(create_table_query)
        print(f"The {table_name} is created")


con=pg_connect(postgresql_database,postgresql_user,postgresql_password,postgresql_host,postgresql_port)
print("Postgresql connection established successfully")
table_name='config_table'
cursor = con.cursor()
create_table(table_name,cursor)
con.commit()
function_query = f"CREATE OR REPLACE FUNCTION update_flag_trigger_function() RETURNS TRIGGER AS $$ BEGIN NEW.update_flag = 'Y'; RETURN NEW; END; $$ LANGUAGE plpgsql;"

trigger_query = f"CREATE TRIGGER update_flag_trigger BEFORE UPDATE ON config_table FOR EACH ROW EXECUTE FUNCTION update_flag_trigger_function();"

cursor.execute(function_query)
cursor.execute(trigger_query)

con.commit()



