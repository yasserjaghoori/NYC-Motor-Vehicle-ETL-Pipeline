# NYC Motor Vehicle Collision – ETL Pipeline
This project automates the ETL (Extract, Transform, Load) process for NYC motor vehicle collision data using AWS services, PySpark, and MySQL (RDS).

# ETL Workflow
- Step 1: Upload Raw File to S3
nyc_etl_upload.py – Loads AWS credentials from config.yaml and uploads the raw CSV file to an S3 bucket.
Command: python3 nyc_etl_upload.py
- Step 2: Data Cleaning & Transformation with PySpark
nyc_etl_spark.py – Reads raw CSV data from S3, cleans data, and writes cleaned data back to S3 as Parquet.
Command: python3 nyc_etl_spark.py
- Step 3: Set Up RDS Database and Table
nyc_rds_setup.py – Connects to your MySQL RDS instance, creates database and collisions table.
Command: python3 nyc_rds_setup.py
Sample config.yaml Format

aws_access_key_id: YOUR_ACCESS_KEY
aws_secret_access_key: YOUR_SECRET_KEY
aws_region: us-east-1
bucket_name: nyc-motor-vehicle-collission

Example Output (Parquet Schema)

| collision_id | crash_date | crash_time | borough | zip_code | latitude | longitude |
|--------------|-------------|------------|---------|----------|----------|-----------|
| 123456789    | 2023-10-15  | 14:35:00   | queens  | 11377    | 40.75123 | -73.90177 |

Author
Yasser Jaghoori
George Mason University – Data Analytics Engineering

