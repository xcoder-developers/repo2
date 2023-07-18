import sys
import boto3
from datetime import datetime, timezone
s3 = boto3.client('s3')
latest_versions = {}

# Extract all latest version of each file
def extractLatestVersions(response,modified_date):
    for version in response['Versions']:
        last_modified = version['LastModified']
        if last_modified < modified_date:
            key = version['Key']
            if key not in latest_versions or latest_versions[key]['LastModified'] < version['LastModified']:
                latest_versions[key] = version

# Copy all latest-version of each file to another location
def copyLatestFiles(bucket_name, output_prefix):
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
        # print(f"Object key: {version['Key']}, version id: {version['VersionId']}, last modified: {version['LastModified']}")

# main method
def main(bucket_name, input_prefix, output_prefix, modified_date):
    paginator = s3.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=input_prefix)
    for page in page_iterator:
        extractLatestVersions(page, modified_date)
    copyLatestFiles(bucket_name,output_prefix)
    

if __name__ == "__main__":
    bucket = sys.argv[1]
    inputPathPrefix = sys.argv[2]
    outputPathPrefix = sys.argv[3]
    date = sys.argv[4]
    modifiedDate=datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
    main(bucket,inputPathPrefix,outputPathPrefix,modifiedDate)
