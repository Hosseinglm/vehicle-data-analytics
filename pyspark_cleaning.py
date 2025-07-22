import ast
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, count, when, isnan, to_timestamp, expr, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, MapType, TimestampType

def main():
    # ---- Glue & Spark Setup ----
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    
    # Set log level to reduce verbosity
    sc.setLogLevel("WARN")
    
    # ---- Read from raw S3 folder as CSV ----
    input_path = "s3://vehicles-datalake/raw/"
    
    # Define schema for the input data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("details", StringType(), True)
    ])
    
    # Read the CSV files with proper options
    df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(input_path)
    

    # Define UDF to parse the details column
    @udf(returnType=MapType(StringType(), StringType()))
    def parse_details_udf(detail_str):
        try:
            if detail_str is None or detail_str == "":
                return {}
            return ast.literal_eval(str(detail_str))
        except (ValueError, SyntaxError) as e:
            return {}
    
    # Apply the UDF to parse details
    df = df.withColumn("parsed_details", parse_details_udf(col("details")))
    
    # Sample the parsed details to extract keys
    sample_df = df.select("parsed_details").limit(1000)
    sample_rows = sample_df.collect()
    
    # Extract all keys from the parsed details
    all_keys = set()
    for row in sample_rows:
        if row.parsed_details:
            all_keys.update(row.parsed_details.keys())
    
  
    # Extract each key from the parsed_details into its own column
    for key in all_keys:
        df = df.withColumn(key, expr(f"parsed_details['{key}']"))
    
    # Drop the original details column and the parsed_details column
    df = df.drop("details", "parsed_details")

    # Define numeric columns
    numeric_cols = ["estimated_speed", "vehicle_class", "frame_number"]
    
    # Convert numeric columns
    for col_name in numeric_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, 
                              when(col(col_name) == "", None)
                              .otherwise(col(col_name).cast(DoubleType())))
    
    # Convert boolean columns
    if "last_appearance" in df.columns:
        df = df.withColumn("last_appearance", col("last_appearance").cast(BooleanType()))
    
    # Define categorical columns
    categorical_cols = ["class_name", "device_name", "zone_label", "tracked_id"]
    
    # Handle missing values for numeric columns
     for col_name in numeric_cols:
        if col_name in df.columns:
            # Count non-null values
            non_null_count = df.filter(~col(col_name).isNull() & ~isnan(col(col_name))).count()
            
            if non_null_count > 0:
                # Calculate median
                median_val = df.select(expr(f"percentile_approx({col_name}, 0.5)")).collect()[0][0]
                # Fill with median
                df = df.withColumn(col_name, when(col(col_name).isNull() | isnan(col(col_name)), median_val).otherwise(col(col_name)))
            else:
                # Fill with 0
                df = df.withColumn(col_name, when(col(col_name).isNull() | isnan(col(col_name)), 0).otherwise(col(col_name)))

    
    # Handle missing values for categorical columns
    for col_name in categorical_cols:
        if col_name in df.columns:
            # Find the mode (most frequent value)
            mode_df = df.groupBy(col_name).count().orderBy(col("count").desc())
            mode_rows = mode_df.filter(~col(col_name).isNull()).limit(1).collect()
            
            if len(mode_rows) > 0:
                mode_val = mode_rows[0][0]
                # Fill with mode
                df = df.withColumn(col_name, when(col(col_name).isNull(), mode_val).otherwise(col(col_name)))
            else:
                # Fill with "unknown"
                df = df.withColumn(col_name, when(col(col_name).isNull(), "unknown").otherwise(col(col_name)))
                
    
    # Check for duplicates
    initial_count = df.count()
    df = df.dropDuplicates()
    final_count = df.count()
    duplicates = initial_count - final_count
    
    # Write the processed data to S3 as Parquet
    parquet_output_path = "s3://vehicles-datalake/processed/parquet/"
    df.write.mode("overwrite").parquet(parquet_output_path)
    
    # Write the processed data to S3 as CSV
    csv_output_path = "s3://vehicles-datalake/processed/csv/"
    df.write.mode("overwrite").option("header", "true").csv(csv_output_path)

    
    # Commit the job
    job.commit()

if __name__ == "__main__":
    main()
