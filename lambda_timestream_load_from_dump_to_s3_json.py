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
This Lambda assumes that the timestream dump json objects have been generated with 'lambda_timestream_dump_to_s3_json.py'.
Retrieve an S3 object name from SQS, load the object from S3 and write the data to timestream.
The destination database and table must be existing.
WARNINGS this lambda:
   Only supports ScalarValue for the Data (and thus expects ScalarType for ColumnInfo.
   Assumes that the time variable is in UTC and format '%Y-%m-%d %H:%M:%S.%nanosecond' -
   Since nanosends is not supported so the string will be truncated to miliseconds

This lambda expects to be called by SQS queue trigger with the following elements the Record messageAttributes:
{"bucket": {"stringValue": "bucket name"}, "key": {"stringValue": "object key"}}

The body can stay empty.

Configuration:
Declare the following environment variables:
:param bool TRACE: True for additional logs
:param str DB: Timestream source database name
:param str TB: Timestream source table name

Create the Timestream Database and Table:
When creating the Timestream Table, enable Magnetic Storage Writes and make sure the Magnetic Store retention period
is longer than the oldest data point you want to ingest.

Tips:
* Increase the execution time of the Lambda function and memory to allow execution on large objects.
* Make sure the Visibility Timeout of the SQS queue is longer than the Lambda execution time.


The Role allocated to the Lambda for execution must have the following policies (or less permissive equivalent):
* AWSLambdaBasicExecutionRole
* AmazonTimestreamFullAccess
* AmazonS3ReadOnlyAccess
* AWSLambdaSQSQueueExecutionRole
"""

import boto3
import os
import json
from dateutil import parser

print("Loading function")

s3 = boto3.client('s3')
tsw = boto3.client('timestream-write')

TRACE = os.environ.get("TRACE", False)
if TRACE in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    TRACE = True
else:
    TRACE = False

DB = os.environ.get("DBNAME")
if not DB:
    raise Exception("Environment variable DBNAME missing")

TB = os.environ.get("DBTABLE")
if not TB:
    raise Exception("Environment variable DBTABLE missing")

# Constants
MAX_RECORDS = 100  # Timestream service quota


def log_me(msg):
    if TRACE is True:
        print(msg)


def check_table_exists(db, tb):
    try:
        _ = tsw.describe_table(DatabaseName=db, TableName=tb)
    except Exception as e:
        msg = "Exception raised when check DB and Table exist: {}".format(e)
        raise RuntimeError(msg)


def get_s3_object(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        obj = json.loads(response['Body'].read())
        return obj
    except Exception as e:
        msg = "Error when fetching the S3 object: {}".format(e)
        raise RuntimeError(msg)


def get_record_template():
    return {
        'Dimensions': [],
        'MeasureName': '',
        'MeasureValue': '',
        'MeasureValueType': '',
        'Time': '',
        'TimeUnit': 'MILLISECONDS'
    }


def get_dim_template():
    return {
        'Name': '',
        'Value': '',
        'DimensionValueType': ''
    }


def get_timestamp(date):
    return int(1000 * parser.parse(date + 'Z').timestamp())


def write_to_timestream(records):
    try:
        response = tsw.write_records(DatabaseName=DB,
                                     TableName=TB,
                                     Records=records)

        log_me("Response from Timestream write: {}".format(response))

    except tsw.exceptions.RejectedRecordsException as e:
        print("Timestream returned a RejectedRecordsException")
        print(e.response)

    except Exception as e:
        msg = "Exception when writing to timestream: {}".format(e)
        raise RuntimeError(msg)


def lambda_handler(event, context):
    log_me("starting")
    check_table_exists(DB, TB)
    for record in event.get('Records', [{}]):
        try:
            bucket = record['messageAttributes']['bucket']['stringValue']
            key = record['messageAttributes']['key']['stringValue']
            if bucket is None or key is None:
                raise RuntimeError("Bucket or Key missing in SQS messageAttributes: "
                                   "bucket='{}', key='{}'".format(bucket, key))
            log_me("Fetching object '{}/{}'".format(bucket, key))
            obj = get_s3_object(bucket, key)
            print("Document '{}/{}' successfully read".format(bucket, key))
            records = []
            print('Found {} rows to ingest'.format(len(obj["Rows"])))
            rows_count = 0
            for row in obj["Rows"]:
                record = get_record_template()
                for idx, data in enumerate(row["Data"]):
                    k, v = list(data.items())[0]
                    name = obj["ColumnInfo"][idx]["Name"]
                    tpe = obj["ColumnInfo"][idx]["Type"]["ScalarType"]
                    if k != "ScalarValue":
                        # NullValues cannot be written to Timestream. Log other types when detected
                        if k != 'NullValue':
                            print("Ignoring unsupported Type: {}".format(k))
                        log_me("Skipping row: index {}, data {}".format(idx, data))
                        continue
                    if name == 'time':
                        # This is a timestamp
                        record["Time"] = str(get_timestamp(v))
                    elif name.startswith("measure_value"):
                        # This is a measured value
                        record["MeasureValue"] = v
                        record["MeasureValueType"] = tpe
                    elif name == 'measure_name':
                        # This is the name of the measurement
                        record["MeasureName"] = v
                    else:
                        # Everything else is a Dimension
                        dim = get_dim_template()
                        dim["Name"] = name
                        dim["Value"] = v
                        dim["DimensionValueType"] = tpe
                        # Add the dimension to the record
                        record["Dimensions"].append(dim)
                # add the record to the records
                records.append(record)
                rows_count += 1
                if len(records) == MAX_RECORDS:
                    # Write to Timestream and reset
                    log_me("Maximum number of {} records reached. Writing to Timestream.".format(MAX_RECORDS))
                    write_to_timestream(records)
                    print("Records written so far: {}".format(rows_count))
                    records = []
            if records:
                # Write to Timestream the last piece
                print("Writing the remaining {} records to Timestream".format(len(records)))
                write_to_timestream(records)
            print("Total number of records written: {}".format(rows_count))
        except Exception as e:
            log_me(e)
