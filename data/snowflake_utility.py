import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.session import Session

import getpass
import pandas as pd
import json

def create_table_edos():
    df_comments=pd.read_csv('sample_comments.csv')
    