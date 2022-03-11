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


import json
import boto3
import os
from datetime import datetime as dt

# Recover & check environment variables
bucket = os.environ.get("BUCKET_NAME")
trace = os.environ.get("TRACE", False)

if trace in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    trace = True
else:
    trace = False

if not bucket:
    raise Exception("Environment variable BUCKET_NAME missing")

s3 = boto3.client('s3')


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    message_ids = []

    if 'Records' in event:
        print("Found {} records to store to S3.".format(len(event['Records'])))
    # First build a list of all the message IDs to process. The list will be depopulated when processed.
    for record in event.get('Records'):
        message_ids.append(record['messageId'])
    if trace:
        print("Messages IDs to proceed: {}".format(message_ids))
    # Process each message in the Records
    for record in event.get('Records'):
        body_str = record.get('body')
        try:
            # Make sure the records is properly structured and the payload exists
            if not body_str:
                raise Exception("No body found in Record")
            body = json.loads(body_str)
            msg = body.get('Message')
            if not msg:
                raise Exception("no Payload found")
            else:
                # Inspect the payload
                payload = json.loads(msg)
                if trace is True:
                    print("The payload is: {}".format(payload))
                timestring = payload.get('timestamp')
                if not timestring:
                    raise Exception('Malformed payload: timestamp key missing')
                thing = payload.get('gateway')
                if not thing:
                    raise Exception('Malformed payload: thing key missing')
                device = payload.get('deviceName')
                if not device:
                    raise Exception('Malformed payload: thing key missing')
                epoch = payload.get('epoch_ms')
                if not epoch:
                    raise Exception('Malformed payload: thing key missing')
                value = payload.get('values')
                if trace is True:
                    print("values in payload: {}".format(value))
                if not value:
                    raise Exception("Empty payload found")
                # Check that the timestamp is in the right format and genera the S3 object key
                tstamp = dt.strptime(timestring, "%Y-%m-%dT%H:%M:%S%z")
                key = "{:02d}/{:02d}/{:02d}/{}/{}/{}.json".format(tstamp.year, tstamp.month, tstamp.day,
                                                                  thing, device, epoch)
                # save to S3
                s3.put_object(
                    Body=json.dumps(payload),
                    Bucket=bucket,
                    Key=key
                )
                if trace is True:
                    print("Object stored: {}".format(key))
                # Finally remove the item from the list of unprocessed messages
                if trace is True:
                    print("Message ID {} processed successfully".format(record['messageId']))
                message_ids.remove(record['messageId'])

        except Exception as e:
            print("Error when processing a Record: {}".format(e))

    r = {"batchItemFailures": [{"itemIdentifier": x} for x in message_ids]}
    if trace is True:
        print("Returning unprocessed messages IDs: {}".format(r))
    return r
