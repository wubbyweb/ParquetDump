import pandas as pd
import sqlite3
import pyarrow.parquet as pq

def parquet_to_sqlite(parquet_file, sqlite_file, table_name):
    # Read the Parquet file
    df = pq.read_table(parquet_file).to_pandas()
    
    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_file)
    
    # Write the data to SQLite
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()

    print(f"Data from {parquet_file} has been imported to {sqlite_file} in table {table_name}")

# Usage
parquet_file = 'path/to/your/file.parquet'
sqlite_file = 'path/to/your/database.db'
table_name = 'your_table_name'

parquet_to_sqlite(parquet_file, sqlite_file, table_name)
