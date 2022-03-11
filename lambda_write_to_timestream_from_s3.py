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
import urllib.parse
import boto3
import os

print('Loading function')

s3 = boto3.client('s3')
ts = boto3.client('timestream-write')


def format_for_timestream(data):
    """
    Formats the data into common attributes and records
    :param data: JSON payload received
    :return: common_att, records
    """

    try:
        common_attr = {
            'Dimensions': [
                {
                    'Name': 'Gateway',
                    'Value': str(data['thing']),
                    'DimensionValueType': 'VARCHAR'
                },
                {
                    'Name': 'deviceName',
                    'Value': str(data['device']),
                    'DimensionValueType': 'VARCHAR'
                },
            ],
            'Time': str(data['epoch_ms']),
            'TimeUnit': 'MILLISECONDS'
        }
        vals = data['values']
        records = []
        for k,v in vals.items():
            r = {
                'MeasureName': k,
                'MeasureValue': str(v),
                'MeasureValueType': 'BIGINT' if isinstance(v, int) else 'DOUBLE'
            }
            records.append(r.copy())
        return common_attr, records
    except Exception as e:
        print("Error when preparing data for timestream: {}".format(e))
        print("Data received: {}".format(data))
        raise e


def write_to_timestream(db, table, common_attributes, records):
    """
    Write the daat to timestream
    :param db: database name
    :param table: table name
    :param dict common_attr: common attributes
    :param list records: records to be written
    :return: None
    """
    try:
        result = ts.write_records(DatabaseName=db,
                                  TableName=table,
                                  Records=records,
                                  CommonAttributes=common_attributes)

        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except ts.exceptions.RejectedRecordsException as err:
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        print("Other records were written successfully. ")
    except Exception as e:
        raise e


def lambda_handler(event, context):
    # Get Environment variables
    print("Starting work")
    try:
        db = os.environ.get("DB_NAME")
        table = os.environ.get("TABLE_NAME")
    except KeyError as e:
        print("Error getting the environment variables: {}".format(e))
        raise e

    try:
        # Get the object from the event and fetch its content
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        response = s3.get_object(Bucket=bucket, Key=key)
        # print("CONTENT TYPE: " + response['ContentType'])
        data = json.loads(response['Body'].read())
        print("New IoT data received: {}".format(data))
    except Exception as e:
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        print(e)
        raise e

    try:
        # Format the data for Timestream and write it
        cattr, recs = format_for_timestream(data)
        write_to_timestream(db=db, table=table, common_attributes=cattr, records=recs)
        print("Finished without error")
    except Exception as e:
        print("Error when pushing data to Timestream")
        print(e)
        raise e
