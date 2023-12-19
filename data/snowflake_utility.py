import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.session import Session
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import getpass
import pandas as pd
import json



def connector():
    
    con = snowflake.connector.connect(
    user='msbeigi',
    password='Ghi9rail755',
    account='xcyevyt-po65493',
    warehouse='COMPUTE_WH',
    database='GIT',
    schema='PUBLIC',
    role='ACCOUNTADMIN'
    )
    
    return con


def create_table_edos():
    df_comments=pd.read_csv('sample_comments.csv')
    con=connector()
    cur=con.cursor()
    # create_command='CREATE TABLE "Odes_Comments" ( "rewire_id" Varchar(30),"text" Varchar(250),"label_sexist" Vachar(30),"label_category" Varchar(30),"label_vector" Varchar(15),"split" Varchar(8),"token" Varchar(30),"target" Varchar(30),"keyword" Varchar(30));'
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
    