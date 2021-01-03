#!/bin/bash
json={\"service\":3}
echo $json | jq
echo ""
echo "Invoking Lambda function - S3_Query using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name S4_TransformLoadQuery --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
