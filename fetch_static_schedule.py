"""
Static GTFS schedule ingestion.
 
Downloads MBTA's published schedule (a zip file of several CSV-like
files), extracts the ones we need, and loads them into Snowflake raw
tables. Unlike the real-time feeds, this only needs to run occasionally
(schedules change every few weeks, not every few minutes).
"""
 
import io
import os
import zipfile
 
import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
 
load_dotenv()
 
STATIC_GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
EXTRACT_DIR = "data/raw/static_gtfs"
 
# Which files we want from the zip, and what Snowflake table each should load into.
FILES_TO_LOAD = {
    "stops.txt": "STOPS",
    "trips.txt": "TRIPS",
    "stop_times.txt": "STOP_TIMES",
}
 
 
def download_and_extract():
    """Download the GTFS zip and extract only the files we need."""
    print("Downloading static GTFS zip...")
    response = requests.get(STATIC_GTFS_URL)
    response.raise_for_status()
 
    os.makedirs(EXTRACT_DIR, exist_ok=True)
 
    # zipfile.ZipFile can read directly from bytes in memory (io.BytesIO),
    # so we don't need to save the zip itself to disk first.
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for filename in FILES_TO_LOAD:
            print(f"Extracting {filename}...")
            z.extract(filename, path=EXTRACT_DIR)
 
 
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
 
 
def load_file_to_snowflake(conn, filename, table_name):
    """Read one extracted CSV file with pandas and bulk-load it into Snowflake."""
    filepath = os.path.join(EXTRACT_DIR, filename)
 
    print(f"Reading {filename} into a DataFrame...")
    df = pd.read_csv(filepath, dtype=str)  # read everything as strings for the raw layer
 
    # Snowflake conventionally expects uppercase column names by default.
    df.columns = [col.upper() for col in df.columns]
 
    print(f"Loading {len(df)} rows into {table_name}...")
    success, _, num_rows, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        auto_create_table=True,
        overwrite=True,  # replace the table's contents each run, since this is a full schedule snapshot
    )
    print(f"Loaded {num_rows} rows into {table_name}. Success: {success}")
 
 
def main():
    download_and_extract()
 
    conn = get_snowflake_connection()
    for filename, table_name in FILES_TO_LOAD.items():
        load_file_to_snowflake(conn, filename, table_name)
    conn.close()
 
    print("Static schedule load complete.")
 
 
if __name__ == "__main__":
    main()
 