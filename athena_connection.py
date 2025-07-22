import boto3
import pandas as pd
import time
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor


def query_athena(query, database, s3_output, region_name="ap-southeast-2"):

    # Create a connection to Athena
    conn = connect(
        s3_staging_dir=s3_output,
        region_name=region_name,
        cursor_class=PandasCursor,
        schema_name=database,
    )

    # Execute the query and return results as a DataFrame
    return pd.read_sql(query, conn)


if __name__ == "__main__":
    # Configuration
    DATABASE = "vehicle_db"  # Replace with your Athena database name
    S3_OUTPUT = "s3://vehicles-datalake/Athena_results/Unsaved/2025/07/09/"
    REGION = "ap-southeast-2"  # Replace with your AWS region

    # Example query - adjust table name as needed
    query = """
    SELECT *
    FROM vehicle_data_processed_parquet
    LIMIT 10
    """

    # Execute the query
    try:
        df = query_athena(query, DATABASE, S3_OUTPUT, REGION)
        print(f"Query returned {len(df)} rows")
        print(df.head())
    except Exception as e:
        print(f"Error executing query: {e}")
