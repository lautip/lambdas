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
Read the totality of the timestream table and store the metadata and rows in JSON format in the destination bucket.
Each json document contains at most BLOCKSIZE rows.
The S3 object keys format is:
    timestream.<DB>.<TB>.<UTC Date & Time>.dump.<page number>.json

this lambda expects to be called by SQS queue trigger with a payload containing as specific day to process, like this:
{'filter': '2022-01-01'}
in the Records.body key of the SQS payload.

Configuration:
Declare the following environment variables:
:param bool TRACE: True for additional logs
:param str BUCKET: Destination bucket name
:param int BLOCKSIZE: The number of pages returned by Timestream packed in a single file. Affects the memory used.
:param str DB: Timestream source database name
:param str TB: Timestream source table name

Tip:
Increase the execution time of the Lambda function and memory to allow execution on large tables.

The Role allocated to the Lambda for execution must have the following policies (or less permissive equivalent):
* AWSLambdaBasicExecutionRole
* AmazonTimestreamReadOnlyAccess
* AmazonS3FullAccess
"""

import json
import boto3
import os
from datetime import datetime as dt

print('Loading function')

s3 = boto3.client('s3')
tsq = boto3.client('timestream-query')

TRACE = os.environ.get("TRACE", False)
if TRACE in ("true", "True", "TRUE", 1, "Yes", "YES", True):
    TRACE = True
else:
    TRACE = False

BUCKET = os.environ.get("BUCKET")
if not BUCKET:
    raise Exception("Environment variable BUCKET missing")

BLOCKSIZE = os.environ.get('BLOCKSIZE')
try:
    BLOCKSIZE = int(BLOCKSIZE)
    if BLOCKSIZE < 1:
        raise Exception('BLOCKSIZE must be larger than 0')
except Exception:
    raise

DB = os.environ.get('DBNAME')
if not DB:
    raise Exception("Environment variable DBNAME missing")
TB = os.environ.get('DBTABLE')
if not TB:
    raise Exception("Environment variable DBTABLE missing")


def print_query_result(query_result):
    print("--- new page ---")
    column_info = query_result['ColumnInfo']
    print("Metadata: {}".format(column_info))
    print("Data: ")
    for row in query_result['Rows']:
        print("Row: {}".format(row))


def save_to_s3(pfx, file_nb, payload):
    print('Storing S3 object #{}'.format(file_nb))
    OBJ_NAME = pfx + '.{:0>8}.json'.format(file_nb)
    s3.put_object(
        Body=json.dumps(payload),
        Bucket=BUCKET,
        Key=OBJ_NAME
    )


def lambda_handler(event, context):
    for record in event.get('Records', [{}]):
        body = json.loads(record.get('body', '{}'))
        filter = body.get('filter')
        if not filter:
            msg = "event not correctly formatted. Read the docstring. \n Event is: {}".format(event)
            print(msg)
            raise RuntimeError(msg)
        OBJ_PFX = 'timestream.{}.{}.{}.{}.dump'.format(DB, TB, filter, dt.utcnow().strftime("%Y-%m-%dT%H-%M-%S"))
        QRY = 'SELECT * FROM "{}"."{}"'.format(DB, TB)
        if filter:
            QRY += " WHERE date_trunc('day', time) = '{}'".format(filter)
        QRY += " ORDER BY time ASC"
        print('Querying Timestream with: {}'.format(QRY))
        paginator = tsq.get_paginator('query')
        page_iterator = paginator.paginate(QueryString=QRY, PaginationConfig={'PageSize': 1000})
        pg_nb = 0
        pg_count = 0
        file_nb = 0
        rows = []
        for page in page_iterator:
            pg_nb += 1
            pg_count += 1
            if TRACE is True:
                print_query_result(page)
            if pg_count == 1:
                # Store the columns info
                columns = page.get("ColumnInfo")
            rows.extend(page.get("Rows"))

            if pg_count >= BLOCKSIZE:
                print("Reached {} pages, rolling to new S3 Object".format(BLOCKSIZE))
                pg_count = 0
                file_nb += 1
                save_to_s3(OBJ_PFX, file_nb, {"Rows": rows, "ColumnInfo": columns})
                rows = []

        if rows:
            file_nb += 1
            save_to_s3(OBJ_PFX, file_nb, {"Rows": rows, "ColumnInfo": columns})
        print('Dump finished without interruption: {} pages processed and {} files written'.format(pg_nb, file_nb))

    return {}
