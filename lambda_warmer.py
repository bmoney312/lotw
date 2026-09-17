import json
import logging
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')

API_FUNCTIONS = [
    'submit_pick',
    'process_pick',
]


def lambda_handler(event, context):
    payload = json.dumps({"warmer": True, "detail-type": "Scheduled Warmer"}).encode('utf-8')

    now = datetime.now()
    month = now.month
    day = now.day

    targets = list(API_FUNCTIONS)
    # add process_registration to warmer during registration periods
    if month == 8 or (month == 9 and 1 <= day <= 15):
        targets.append('process_registration')

    logger.info("Current date is %02d/%02d. Warming targets: %s", month, day, targets)

    for function_name in targets:
        try:
            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',
                Payload=payload
            )
        except Exception as e:
            logger.error("Error invoking %s: %s", function_name, str(e))

    return {"status": "Warming signals dispatched", "targets": targets}
