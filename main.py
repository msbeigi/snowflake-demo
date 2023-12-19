import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructField, StructType, IntegerType, StringType, VariantType
from snowflake.snowpark.session import Session
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import getpass
import pandas as pd
import json


if (__name__=='__main__'):



