import boto3

client = boto3.client("kinesis", region_name="ap-southeast-2")
print(client.list_streams())
