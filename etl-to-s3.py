from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from datetime import datetime
import uuid

# Load environment variables from .env
load_dotenv()

# PostgreSQL connection details
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)

# SQL to get recent data (last 5 minutes)
query = """
SELECT * FROM transactions
WHERE last_updated > NOW() - interval '5 minutes'
ORDER BY last_updated;
"""

# Read data into pandas DataFrame
df = pd.read_sql(query, conn)

if not df.empty:
    # Transformations
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.dropna()

    # Build S3 path with date partitions
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')
    s3_path = f"s3://{os.getenv('S3_BUCKET')}/raw/{today_str}/part-{uuid.uuid4()}.parquet"



    # Write to S3 in Parquet format
    df.to_parquet(s3_path, engine='pyarrow', index=False)
    print(f"✅ Uploaded {len(df)} rows to {s3_path}")
else:
    print("ℹ️ No new data to upload.")

conn.close()
