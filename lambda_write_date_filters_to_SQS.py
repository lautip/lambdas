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
This lambda populates a SQS queue with a message containing a single date. It is to be used in conjuction with
lambda_timestream_dump_to_s3_json.py to generate per-day Timestream dump files.
The massage is formatted to contain a string reprsenting the following dictionary:
{'filter': 'date'} with date format: "%Y-%m-%d"

Configuration:
Create a SQS queues with batch = 1
Declare the following environment variables:
:param bool TRACE: True for additional logs

The Role allocated to the Lambda for execution must have the following policies (or less permissive equivalent):
* AWSLambdaBasicExecutionRole
* AmazonSQSFullAccess

"""

import json
import boto3
import os
import datetime as dt

START_DAY = os.environ.get("START_DAY")
if not START_DAY:
    msg = "Missing environment variable 'START_DAY"
    print(msg)
    raise RuntimeError(msg)

END_DAY = os.environ.get("END_DAY")
if not START_DAY:
    msg = "Missing environment variable 'START_DAY"
    print(msg)
    raise RuntimeError(msg)

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

sqs = boto3.client('sqs')


def log_me(msg):
    if TRACE is True:
        print(msg)


def lambda_handler(event, context):
    try:
        current = dt.datetime.strptime(START_DAY, "%Y-%m-%d")
        end = dt.datetime.strptime(END_DAY, "%Y-%m-%d")
        log_me("Using SQS URL: '{}'".format(SQS_URL))
        while current <= end:
            d = {'filter': current.strftime("%Y-%m-%d")}
            response = sqs.send_message(
                QueueUrl=SQS_URL,
                MessageBody=json.dumps(d),
                DelaySeconds=0
            )
            log_me(response)
            current += dt.timedelta(days=1)
    except Exception as e:
        print("Exception during runtime: {}".format(e))
        raise
    else:
        print("Finished without error")

