#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import zipfile
import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import os


# In[47]:


# Define the URL you want to request
url = 'http://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'

# Specify the directory where you want to save the zip file
save_directory = 'D:/Black Coffer/AWS Company files'

# Define the full file path including the directory and file name
save_path = os.path.join(save_directory, 'companyfacts.zip')

# Define custom user agent for Google Chrome
custom_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

# Define request headers with custom user agent
headers = {'User-Agent': custom_user_agent}

# Make an HTTP GET request with custom user agent
response = requests.get(url, headers=headers)


with open(save_path, 'wb') as f:
    f.write(response.content)


# In[48]:


# Open an existing ZIP file for reading for Extraction
with zipfile.ZipFile('D:/Black Coffer/AWS Company files/companyfacts.zip', 'r') as zip_ref:
    zip_ref.extractall("D:/Black Coffer/AWS Company files/companyfacts_unziped")


# In[4]:


# Create an S3 client
s3 = boto3.client('s3',
                  aws_access_key_id='XXXXXXXXXXXXXXX',
                  aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXX')

# Specify the name of the bucket and the directory containing the files you want to upload
bucket_name = 'aws-project-978'
directory_path = "D:/Black Coffer/AWS Company files/companyfacts_unziped"

# List all files in the directory
for filename in os.listdir(directory_path):
    file_path = os.path.join(directory_path, filename)
    
    # Upload each file to the specified bucket with a unique key
    s3.upload_file(file_path, bucket_name, filename)
    #print(f"Uploaded {filename} to {bucket_name}")


# In[25]:


import random
# Initialize AWS clients
session = boto3.Session(
    aws_access_key_id='XXXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    region_name='us-east-1'
)

s3_client = session.client('s3')
dynamodb_client = session.client('dynamodb')

# Define S3 bucket and DynamoDB table names
s3_bucket_name = 'aws-project-978'
dynamodb_table_name = 'AWS-NoSQL-Database'

# Retrieve a list of objects from the specified S3 bucket
response = s3_client.list_objects_v2(Bucket=s3_bucket_name)
objects = response.get('Contents', [])

# Iterate through each object, retrieve its data from S3, parse the data (assuming it's JSON), and store it into DynamoDB
for obj in objects:
    # Retrieve object data from S3
    object_key = obj['Key']
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=object_key)
    
    # Parse object data (assuming it's JSON)
    object_data = response['Body'].read().decode('utf-8')
    item = json.loads(object_data)

    # Check if 'facts' key exists in the item dictionary
    if 'facts' not in item:
        continue

    # Flatten nested maps and remove additional keys
    flattened_item = {}
    for category, attributes in item.get('facts', {}).items():
        for attribute, value in attributes.items():
            # Check if the value is a valid type for DynamoDB
            if isinstance(value, (str, int, float)):
                flattened_item[f"{category}.{attribute}"] = value

    # Generate a random 4-digit number for 'cik'
    random_key = str(random.randint(1000, 9999))

    if "facts":
        dynamodb_item = {
            '0001': {'N': random_key},  # Using the random number as the primary key
            'cik': {'N': str(item['cik'])},  # Assuming cik is a number attribute
            'entityName': {'S': item['entityName']},# Assuming entityName is a string attribute
        }

    # Store item in DynamoDB table
    dynamodb_client.put_item(TableName=dynamodb_table_name, Item=dynamodb_item)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




