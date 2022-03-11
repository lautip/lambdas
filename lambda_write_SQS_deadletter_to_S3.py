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
import time

# Recover & check environment variables
bucket = os.environ.get("DEADLETTER_BUCKET_NAME")
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
    # save to S3 without further processing
    key = str(time.time_ns())
    s3.put_object(
        Body=json.dumps(event),
        Bucket=bucket,
        Key=key
    )
