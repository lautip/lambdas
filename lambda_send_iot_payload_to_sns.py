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
:param TRACE: True for additional logs. Supports multiple formats. Check the code!

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

# Set the gateway data model version(s) compatible with this version of the Lambda as a Tuple
DATA_MODEL_VER = ('ekip0001',)
# Set the data contract version to add to the output payload as a String
DATA_CONTRACT_VER = 'ekip0001'

# A few useful constants
KEY_TSTAMP = 'timestamp'  # The key for the timestamp
KEY_DATA = 'dataObject'
KEY_DATA_MODEL = 'data_model'  # The key for the data model version received from the gateway
KEY_EPOCH = 'epoch_ms'
KEY_SITE = 'site_name'
KEY_DEVICE = 'device_name'

KEY_CONTRACT = 'contract'  # The key for the contract version in the output payload

# Keys expected in the input payload as a Set
KEYS_EXPECTED = {KEY_TSTAMP, KEY_DATA, KEY_EPOCH, KEY_SITE, KEY_DEVICE, KEY_DATA_MODEL}


def log_me(msg):
    if TRACE is True:
        print(msg)


def validate_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise RuntimeError("The payload should be a dictionary but a type '{}' was received".format(type(payload)))
    if not payload.get(KEY_DATA_MODEL) in DATA_MODEL_VER:
        raise RuntimeError("Unsupported version of the incoming data model: '{}'".format(payload.get(KEY_DATA_MODEL)))
    # Optional - could become a problem if this Lambda supports multiple payloads
    if not payload.keys() == KEYS_EXPECTED:
        raise RuntimeError("The input payload keys does not comply with the expected keys. "
                           "Received keys: {} / Expected keys: {}".format(sorted(payload.keys()),
                                                                          sorted(KEYS_EXPECTED)))
    log_me("The input payload is compliant.")


def adjust_payload(payload: dict) -> dict:
    validate_payload(payload)
    payload.pop(KEY_TSTAMP)
    payload.pop(KEY_DATA_MODEL)
    payload[KEY_CONTRACT] = DATA_CONTRACT_VER
    return payload


def lambda_handler(event, context):
    try:
        log_me("Received event: {}".format(event))
        payload = adjust_payload(event)

        log_me("Sending to SNS: {}".format(payload))
        # Publish the formatted message
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload)
        )
        log_me("SNS publish response: {}".format(response))

    except Exception as e:
        print(e)
        # Store event in the Deadletter bucket
        msg = {
            'error': str(e),
            'event': json.dumps(event)
        }
        s3_key = "lambda-{}/{}.json".format(MYSELF, int(time.time() * 1000))
        s3.put_object(
            Body=json.dumps(msg),
            Bucket=DEAD_BUCKET,
            Key=s3_key
        )
        print("Details stored to S3 object: {}".format(s3_key))
