import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')

# Set the modified date filter
modified_after = datetime(2022, 1, 1, tzinfo=timezone.utc)

# List all object versions modified after the specified date
response = s3.list_object_versions(
    Bucket='bucket-name',
    Prefix='path/to/objects/',
    Filters={'LastModified': {'GreaterThan': modified_after}}
)

# Print the key and version ID of each object version
for version in response['Versions']:
    print(f"Object key: {version['Key']}, version id: {version['VersionId']}")



import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')

# Set the modified date filter
modified_after = datetime(2022, 1, 1, tzinfo=timezone.utc)

# List all object versions modified after the specified date
response = s3.list_object_versions(
    Bucket='bucket-name'
)

# Print the key and version ID of each object version
for version in response['Versions']:
    last_modified = version['LastModified']
    if last_modified > modified_after:
        print(f"Object key: {version['Key']}, version id: {version['VersionId']}")
