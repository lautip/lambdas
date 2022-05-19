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
This lambda reads a payload received from SQS and expects to find a JSON object in the Message.
For testing you can use the JSON object below, simulating the payload sent by SQS.

{
  "Records": [
    {
      "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
      "receiptHandle": "MessageReceiptHandle",
      "body": "{\"Message\": \"{\\\"Hello\\\": \\\"from SQS!\\\"}\"}",
      "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1523232000000",
        "SenderId": "123456789012",
        "ApproximateFirstReceiveTimestamp": "1523232000001"
      },
      "messageAttributes": {},
      "md5OfBody": "{{{md5_of_body}}}",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
      "awsRegion": "us-east-1"
    }
  ]
}

"""

import json
import time

import boto3
import os

# Recover & check environment variables
bucket = os.environ.get("BUCKET_NAME")
trace = os.environ.get("TRACE", True)
if trace in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    trace = True
else:
    trace = False

inspect = os.environ.get("INSPECT", False)
if inspect in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    inspect = True
else:
    inspect = False

if not bucket:
    raise Exception("Environment variable BUCKET_NAME missing")

s3 = boto3.client('s3')


def log_me(msg):
    if trace is True:
        print(msg)


def lambda_handler(event, context):
    message_ids = []

    if 'Records' in event:
        log_me("Found {} records to store to S3.".format(len(event['Records'])))
    # First build a list of all the message IDs to process. The list will be depopulated when processed.
    for record in event.get('Records'):
        message_ids.append(record['messageId'])
    log_me("Messages IDs to proceed: {}".format(message_ids))
    # Process each message in the Records
    for record in event.get('Records'):
        # log_me("Record is: {}".format(record))
        body_str = record.get('body')
        # log_me("body_str type: {}, value: ...{}...".format(type(body_str), body_str))
        try:
            # Make sure the records is properly structured and the payload exists
            if not body_str:
                raise Exception("No body found in Record")
            log_me("loading body")
            body = json.loads(body_str)
            log_me("getting Message")
            msg = body.get('Message')
            # log_me("Message type is {} and value is {}".format(type(msg), msg))
            if not msg:
                raise Exception("no Payload found")
            else:
                log_me('Getting the payload')
                payload = json.loads(msg)
                log_me("The payload is: {}".format(payload))

                # save to S3
                key = '{}.json'.format(time.time_ns())
                s3.put_object(
                    Body=json.dumps(payload),
                    Bucket=bucket,
                    Key=key
                )

                log_me("Object stored: {}".format(key))
                # Finally, remove the item from the list of unprocessed messages
                log_me("Message ID {} processed successfully".format(record['messageId']))
                message_ids.remove(record['messageId'])

        except Exception as e:
            print("Error when processing a Record: {}".format(e))

    r = {"batchItemFailures": [{"itemIdentifier": x} for x in message_ids]}
    log_me("Returning unprocessed messages IDs: {}".format(r))
    return r
