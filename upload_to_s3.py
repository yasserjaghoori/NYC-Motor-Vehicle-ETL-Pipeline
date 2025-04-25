#!/usr/bin/env python
# coding: utf-8

import boto3
import yaml
import os

# 1) Load credentials from your LOCAL config.yaml
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

aws_access_key_id     = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
aws_region            = config['aws_region']
bucket_name           = config['bucket_name']
local_file_path       = 'file_path'
s3_file_path          = 's3_file_path'

# 2) Init boto3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# 3) Upload
s3.upload_file(local_file_path, bucket_name, s3_file_path)
print(f"File uploaded successfully to s3://{bucket_name}/{s3_file_path}")
