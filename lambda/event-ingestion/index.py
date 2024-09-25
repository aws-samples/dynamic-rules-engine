""" 
Copyright 2024 Amazon.com, Inc. and its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

  http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
"""

import time
import boto3
import json
import logging
import os
import traceback

# region Logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger()

if logger.hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logger.setLevel(LOG_LEVEL)
else:
    logging.basicConfig(level=LOG_LEVEL)

# endregion

# region Kinesis

kinesis = boto3.client("kinesis")
KINESIS_STREAM_ARN = os.getenv("KINESIS_STREAM_ARN", None)
if not KINESIS_STREAM_ARN:
    raise ValueError("KINESIS_STREAM_ARN environment variable is not set")
else:
    logger.info(f"Kinesis stream ARN: {KINESIS_STREAM_ARN}")

# endregion


def mask_sensitive_data(event):
    # remove sensitive data from request object before logging
    keys_to_redact = ["authorization"]
    result = {}
    for k, v in event.items():
        if isinstance(v, dict):
            result[k] = mask_sensitive_data(v)
        elif k in keys_to_redact:
            result[k] = "<redacted>"
        else:
            result[k] = v
    return result


def build_response(http_code, body):
    return {
        "headers": {
            "Cache-Control": "no-cache, no-store",
            "Content-Type": "application/json",
        },
        "statusCode": http_code,
        "body": body,
    }


def stream_to_kinesis(batch):
    logger.info(f"Streaming batch of {len(batch)} records to Kinesis")

    for record in batch:
        if 'id' in record:
            partition_key = record['id']
        else:
            partition_key = str(int(time.time()))
        response = kinesis.put_record(
            Data=json.dumps(record).encode('utf-8'),
            PartitionKey=partition_key,
            StreamARN=KINESIS_STREAM_ARN
        )
        logger.info(response)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception("Failed to stream record to Kinesis")


def lambda_handler(event, context):
    logger.info(mask_sensitive_data(event))
    try:
        stream_to_kinesis(event["Records"])
    except Exception as ex:
        logger.error(traceback.format_exc())


if __name__ == "__main__":

    example_event = {}
    response = lambda_handler(example_event, {})
    print(json.dumps(response))
