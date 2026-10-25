# Copyright © Sachin Chandrashekhar - Data Engineering Hub. All Rights Reserved.
#
# This code is provided as part of a paid training program.
# Unauthorized copying, distribution, or
# publication of this code—either in whole or in part—
# is strictly prohibited.
#
# Licensed for personal educational use only.
# If this code is found on a public repository or distributed without
# authorization, please notify: legal@dataengineeringhub.in

import boto3
import json
import os

def lambda_handler(event, context):
    sfn = boto3.client('stepfunctions')
    state_machine_arn = os.environ['STATE_MACHINE_ARN']
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(f"New file detected: s3://{bucket}/{key}")
    
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({"trigger": "s3_event", "source_file": key})
    )
    
    print(f"Started Step Function execution: {response['executionArn']}")
    return {"statusCode": 200, "executionArn": response['executionArn']}
