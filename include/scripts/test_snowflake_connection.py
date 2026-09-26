"""
Test script: confirms that Python can authenticate to Snowflake
using the credentials stored in .env, before we build any real
data-loading logic on top of it.
"""

import os

import snowflake.connector
from dotenv import load_dotenv

# Reads the .env file and loads its key=value pairs as environment
# variables, accessible via os.getenv() below.
load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
result = cursor.fetchone()

print(f"Connected successfully! Snowflake version: {result[0]}")

cursor.close()
conn.close()