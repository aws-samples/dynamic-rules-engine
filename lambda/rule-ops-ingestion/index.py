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

import base64
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


def lambda_handler(event, context):
    logger.info(f"Lambda got the following event:\n {event}")
    try:
        # Read the records from Kinesis
        records = event["Records"]
        logger.info(f"Processing {len(records)} records...")

        # Iterate through each record
        for record in records:
            # Get the payload from the record
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            # Parse the payload as JSON
            payload_json = json.loads(payload)
            # Log payload
            logger.info(mask_sensitive_data(payload_json))
    except Exception as ex:
        logger.error(traceback.format_exc())


if __name__ == "__main__":

    example_event = {}
    response = lambda_handler(example_event, {})
    print(json.dumps(response))
