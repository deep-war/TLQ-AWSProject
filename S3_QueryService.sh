#!/bin/bash
echo "Invoking Lambda function - S3_Query using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name S3_Query --region us-east-2 /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
