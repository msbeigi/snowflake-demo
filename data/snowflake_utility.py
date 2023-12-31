import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.session import Session
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import getpass
import pandas as pd
import json
import os

CONFIG={}


def setup():
    """
    Reads Snowflake connection details from config.json file.

    Returns:
        dict: Configuration details for Snowflake connection.
    """
    with open('./config.json') as f:
        config_data=json.load(f)
    snowflake_config=config_data.get('snowflake',{})
    keys = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']

    for key in keys:
        CONFIG[key] = snowflake_config.get(key)
    
    return CONFIG#snowflake_config.get('user')
    

def connector():
    """
    Establishes a connection to Snowflake using the configuration details.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: Snowflake connection object.
    """
    setup()
    con = snowflake.connector.connect(
    user=CONFIG['user'],
    password=CONFIG['password'],
    account=CONFIG['account'],
    warehouse=CONFIG['warehouse'],
    database=CONFIG['database'],
    schema=CONFIG['schema'],
    role=CONFIG['role']
    )
    
    return con


def create_table_edos():
    """
        Creates a table 'Odes_Comments' in Snowflake if it does not exist.

        Returns:
            None
        """
    
    con=connector()
    cur=con.cursor()

    table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{CONFIG['schema']}' AND TABLE_NAME = 'Odes_Comments'"
    cur.execute(table_exists_query)
    table_exists = cur.fetchone()[0]

    if table_exists == 0:
        create_command='CREATE TABLE "Odes_Comments" (  \
                                "rewire_id" VARCHAR,    \
                                "text" VARCHAR,    \
                                "label_sexist" VARCHAR,    \
                                "label_category" VARCHAR,    \
                                "label_vector" VARCHAR,    \
                                "split" VARCHAR,    \
                                "token" VARCHAR,    \
                                "target" VARCHAR,    \
                                "keyword" VARCHAR    \
                            );'

        cur.execute(create_command)
        print("Table 'Odes_Comments' created.")
    else:
        print("Table 'Odes_Comments' exists.")
    cur.close()
    con.close()


def load_csv_file():
    """
    Loads a CSV file 'sample_comments.csv' from the data directory.

    Returns:
        pandas.DataFrame: DataFrame containing the loaded data.
    """
    file_path = './data/sample_comments.csv'
    df = pd.read_csv(file_path)

    return df

def insert_file_odes():
    """
    Inserts data from the CSV file into the 'Odes_Comments' table in Snowflake.

    Returns:
        None
    """
    df_comments=load_csv_file()
    conn=connector()
    cur=conn.cursor()

    select_command='SELECT * FROM GIT.PUBLIC."Odes_Comments"'
    cur.execute(select_command)
    data=cur.fetchall()
    
    if not data:
        # print(len(pd.DataFrame(data)))
        write_pandas(conn,df_comments,table_name='Odes_Comments')
    else:
        print('Data already in the table.')
    cur.close()
    conn.close()