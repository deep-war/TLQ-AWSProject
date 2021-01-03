#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"bucketname\"":\"termproj.562f20.edw\"","\"filename\"":\"TRANSFORMED_CSV.csv\""}
echo $json | jq
echo ""
echo "Invoking Lambda function - S2_Load using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name S2_Load --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
