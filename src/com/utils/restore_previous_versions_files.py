"""
Script to extract the latest versions of files from an S3 bucket
and copy them to another location.

Author: Ravi Kushwah
Date: 29-06-2023
"""
import sys
from datetime import datetime, timezone
import boto3
s3 = boto3.client('s3')
latest_versions = {}

def extract_latest_versions(response,modified_date):
    """
    Extracts the latest version of each file from the S3 response.

    Args:
        response (dict): The S3 response containing the file versions.
        modified_date (datetime): The modified date threshold.

    """
    for version in response['Versions']:
        last_modified = version['LastModified']
        if last_modified < modified_date:
            key = version['Key']
            if key not in latest_versions or latest_versions[key]['LastModified'] \
            < version['LastModified']:
                latest_versions[key] = version

def copy_latest_files(bucket_name, output_prefix):
    """
    Copies the latest version of each file to the specified output location.

    Args:
        bucket_name (str): The name of the S3 bucket.
        output_prefix (str): The output prefix for the copied files.

    """
    for version in latest_versions.values():
        file_path = version['Key']
        path_parts = file_path.split('/')
        s3.copy_object(
        Bucket=bucket_name,
        CopySource={
            'Bucket': bucket_name,
            'Key': version['Key'],
            'VersionId': version['VersionId']
        },
        Key=output_prefix+'/'+path_parts[-1]
        )

def main(bucket_name, input_prefix, output_prefix, modified_date):
    """
    Main method for extracting and copying the latest file versions.

    Args:
        bucket_name (str): The name of the S3 bucket.
        input_prefix (str): The input prefix for the files.
        output_prefix (str): The output prefix for the copied files.
        modified_date (datetime): The modified date threshold.

    """
    paginator = s3.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket = bucket_name, Prefix = input_prefix)
    for page in page_iterator:
        extract_latest_versions(page, modified_date)
    copy_latest_files(bucket_name,output_prefix)

if __name__ == "__main__":
     # Command-line arguments: <bucket_name> <input_prefix> <output_prefix> <modified_date>
    BUCKET = sys.argv[1]
    inputPathPrefix = sys.argv[2]
    outputPathPrefix = sys.argv[3]
    date = sys.argv[4]
    modifiedDate=datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
    main(BUCKET,inputPathPrefix,outputPathPrefix,modifiedDate)
