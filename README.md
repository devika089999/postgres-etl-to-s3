This project demonstrates a real-time ETL pipeline that extracts data from a PostgreSQL database (managed via pgAdmin), transforms it with Python and Pandas, and uploads it to an Amazon S3 data lake in Parquet format.

The workflow includes:
Creating and managing PostgreSQL databases and tables in pgAdmin
Setting up AWS resources including IAM user and S3 bucket
Managing sensitive credentials securely using a .env file
Extracting data from PostgreSQL using psycopg2 and transforming with Pandas
Uploading transformed Parquet files to S3 
