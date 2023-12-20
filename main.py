import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.session import Session
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import getpass
import pandas as pd
import json
from data.snowflake_utility import setup,create_table_edos,insert_file_odes,load_csv_file

if (__name__=='__main__'):
    # create_table_edos()
    # insert_file_odes()
    insert_file_odes()
    # print(d.iloc[:1])


