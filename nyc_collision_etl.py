from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os 
# then create spark session
spark = SparkSession.builder \
    .appName("Advanced NYC Collision ETL") \
    .config("spark.hadoop.fs.s3a.impl",      "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.endpoint",   "s3.amazonaws.com") \
    .getOrCreate()

# Step 2: Define schema manually (cleaner than inferSchema)
collision_schema = StructType([
    StructField("COLLISION_ID", IntegerType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("CRASH_TIME", StringType(), True),
    StructField("BOROUGH", StringType(), True),
    StructField("ZIP_CODE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True)
])

# Step 3: Read raw data from S3
raw_df = spark.read.csv(
    "s3_filepath",
    header=True,
    schema=collision_schema
)

# Step 4: Clean and transform data
cleaned_df = raw_df.dropna(subset=["COLLISION_ID", "CRASH_DATE"]) \
    .withColumn("crash_date", to_date(col("CRASH_DATE"), "MM/dd/yyyy")) \
    .withColumn("crash_time", to_timestamp(col("CRASH_TIME"), "HH:mm")) \
    .withColumn("borough", lower(col("BOROUGH"))) \
    .drop("CRASH_DATE", "CRASH_TIME")

# Step 5: Write processed data back to S3 as Parquet
cleaned_df.write.parquet(
    "s3_filepath",
    mode="overwrite"
)

print("Advanced ETL process completed successfully!")

# Step 6: Stop Spark session
spark.stop()
