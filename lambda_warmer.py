import boto3
import json

lambda_client = boto3.client('lambda')

API_FUNCTIONS = [
    'submit_pick',
    'process_pick',
    'process_registration',
]


def lambda_handler(event, context):
    payload = json.dumps({"warmer": True, "detail-type": "Scheduled Warmer"}).encode('utf-8')

    for function_name in API_FUNCTIONS:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Asynchronous (fire-and-forget)
            Payload=payload
        )

    return {"status": "Warming signals dispatched"}
