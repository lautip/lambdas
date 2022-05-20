# Copyright 2010-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.


"""
This lambda published the bucket name and object keys (*.json) to SQS. Each object is published in a separate message.
The bucket name and object key are placed in Record messageAttributes as:
{"bucket": "bucket name", "key": "object key"}

Configuration:
Declare the following environment variables:
:param bool TRACE: True for additional logs
:param str BUCKET_NAME: Destination bucket name

The Role allocated to the Lambda for execution must have the following policies (or less permissive equivalent):
* AWSLambdaBasicExecutionRole
* AmazonSQSFullAccess
* AmazonS3ReadOnlyAccess

"""

import boto3
import os


SQS_URL = os.environ.get("SQS_URL")
if not SQS_URL:
    msg = "Missing environment variable 'SQS_URL"
    print(msg)
    raise RuntimeError(msg)

TRACE = os.environ.get("TRACE", True)
if TRACE in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    TRACE = True
else:
    TRACE = False

BUCKET = os.environ.get("BUCKET_NAME")
if not BUCKET:
    raise Exception("Environment variable BUCKET_NAME missing")

s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def log_me(msg):
    if TRACE is True:
        print(msg)


def get_formatted_sqs_attributes(key, bucket=BUCKET):
    return {
        'bucket': {
            'StringValue': str(bucket),
            'DataType': 'String'
        },
        'key': {
            'StringValue': str(key),
            'DataType': 'String'
        }
    }


def lambda_handler(event, context):
    response = s3.list_objects(
        Bucket=str(BUCKET),
    )
    for content in response['Contents']:
        if not content['Key'].endswith('.json'):
            log_me("Skipping key '{}'".format(content['Key']))
            continue
        log_me("Publishing: {}".format(content['Key']))
        response = sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody='See messageAttributes',
            DelaySeconds=0,
            MessageAttributes=get_formatted_sqs_attributes(content['Key'])
        )
        log_me(response)