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
What does this Lambda do:
Modifies the payload received from IoT Rule and passes the result to an SNS topic.
In case of exception during the runtime the error and the event are stored in an S3 object in JSON format.

Configuration
Declare the following environment variables:
:param str SNS_TOPIC_ARN: The ARN of the destination SNS topic
:param str DEAD_LETTER_S3_BUCKET: Destination bucket for storing failed transactions
:param bool TRACE: True for additional logs

The Role allocated to this Lambda for execution must have the following policies (or less permissive equivalent):
* 'AWSLambdaBasicExecution'-> for Logging to CloudWatch
* Publish to SNS topic
* Write to S3 bucket
"""

import json
import boto3
import os
import time

# Declare the required service interfaces
sns = boto3.client('sns')
s3 = boto3.client('s3')

# Grab the environment variables set by the User
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
DEAD_BUCKET = os.environ.get('DEAD_LETTER_S3_BUCKET')
TRACE = os.environ.get("TRACE", False)
if TRACE in ("true", "True", "TRUE", 1, "Yes", "YES", "yes", True, "T", "Y", "y"):
    TRACE = True
else:
    TRACE = False

# Grab the environment variables automatically set
MYSELF = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

# Set the gateway data model version(s) compatible with this version of the Lambda
DATA_MODEL_VER = ('myModelVersion',)
# Set the data contract version to add to the output payload
DATA_CONTRACT_VER = 'ekip00001'

# A few useful constants
KEY_TSTAMP = 'timestamp'  # The key for the timestamp
KEY_CONTRACT = 'data-contract'  # The key for the contract version
KEY_DATA_MODEL_VER = 'data-model'  # The key for the data model version received from the gateway


def log_me(msg):
    if TRACE is True:
        print(msg)


def lambda_handler(event, context):
    try:
        print("Received event: {}".format(event))
        if not event.get('values'):
            raise AttributeError("Bad incoming payload!")

        # Remove this line when the right payload is received
        event[KEY_DATA_MODEL_VER] = DATA_MODEL_VER[0]

        # Check that the data model received is compatible with this Lambda version and remove from payload
        model_ver = event.pop(KEY_DATA_MODEL_VER, None)
        if model_ver not in DATA_MODEL_VER:
            raise RuntimeError('Data Model Version not supported: {}'.format(model_ver))

        # Add the Data Contract Version to the payload
        event[KEY_CONTRACT] = DATA_CONTRACT_VER

        log_me("Sending to SNS: {}".format(event))
        # Publish the formatted message
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(event)
        )
        log_me("SNS publish response: {}".format(response))
    except Exception as e:
        print(e)
        # Store event in the bucket
        payload = {
            'error': str(e),
            'event': json.dumps(event)
        }
        s3_key = "lambda-{}/{}.json".format(MYSELF, int(time.time()*1000))
        s3.put_object(
            Body=json.dumps(payload),
            Bucket=DEAD_BUCKET,
            Key=s3_key
        )
        print("Details stored to S3 object: {}".format(s3_key))
