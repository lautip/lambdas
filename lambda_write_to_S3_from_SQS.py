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
import datetime as dt

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
cw = boto3.client('cloudwatch')

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def log_me(msg):
    if trace is True:
        print(msg)


def send_cw_metric(name, dims, unit, value, namespace):
    response = cw.put_metric_data(
        MetricData=[
            {
                'MetricName': name,
                'Dimensions': dims,
                'Unit': unit,
                'Value': value,
                'Timestamp': dt.datetime.utcnow()
            },
        ],
        Namespace=namespace
    )
    log_me(response)


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    message_ids = []

    if 'Records' in event:
        print("Found {} records to store to S3.".format(len(event['Records'])))
        dims = [{'Name': 'Function', 'Value': 'SQS_to_S3'}]
        send_cw_metric(name='Batch Size', dims=dims, unit='Count', value=len(event['Records']),
                       namespace='Custom Lambda Metrics')
    # First build a list of all the message IDs to process. The list will be depopulated when processed.
    for record in event.get('Records'):
        message_ids.append(record['messageId'])
    log_me("Messages IDs to proceed: {}".format(message_ids))
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
                payload = json.loads(msg)
                log_me("The payload is: {}".format(payload))
                if inspect is True:
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
                    log_me("values in payload: {}".format(value))
                    if not value:
                        raise Exception("Empty payload found")
                    # Check that the timestamp is in the right format and genera the S3 object key
                    tstamp = dt.datetime.strptime(timestring, TIME_FORMAT)
                else:
                    # Do not inspect payload - try to retrieve timestamp in ms or generate it
                    epoch = payload.get('epoch_ms', int(dt.datetime.utcnow().timestamp()*1000))
                    thing = payload.get('gateway', payload.get('device_name', 'unknown_gateway'))
                    device = payload.get('deviceName',  payload.get('site_name', 'unknown_device'))
                    tstamp = dt.datetime.fromtimestamp(epoch/1000, dt.timezone.utc)

                # save to S3
                key = "{:02d}/{:02d}/{:02d}/{}/{}/{}.json".format(tstamp.year, tstamp.month, tstamp.day,
                                                                  thing, device, epoch)
                s3.put_object(
                    Body=json.dumps(payload),
                    Bucket=bucket,
                    Key=key
                )
                log_me("Object stored: {}".format(key))
                # Finally remove the item from the list of unprocessed messages
                log_me("Message ID {} processed successfully".format(record['messageId']))
                message_ids.remove(record['messageId'])

        except Exception as e:
            print("Error when processing a Record: {}".format(e))

    r = {"batchItemFailures": [{"itemIdentifier": x} for x in message_ids]}
    log_me("Returning unprocessed messages IDs: {}".format(r))
    return r
