#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/../.env

aws cloudformation describe-stacks --profile p2r --stack-name p2r
if [ $? -ne 0 ]; then
    echo "Stack does not exist. Exiting."
exit 1
fi

S3_DATABASE_EXPORT_BUCKET=$(aws cloudformation describe-stack-resources --profile p2r --stack-name p2r --logical-resource-id s3Bucket | jq -r ".StackResources[0].PhysicalResourceId")
if [ $? -ne 0 ]; then
    echo ${S3_DATABASE_EXPORT_BUCKET}
    exit $?
fi

# Make sure we're trying to empty the right bucket
echo ${S3_DATABASE_EXPORT_BUCKET} | grep -e '^p2r-s3bucket'
if [ $? -ne 0 ]; then
    echo ${S3_DATABASE_EXPORT_BUCKET}
    exit $?
fi
aws s3 rm s3://${S3_DATABASE_EXPORT_BUCKET} --recursive --profile p2r

aws cloudformation delete-stack --profile p2r --stack-name p2r
if [ $? -ne 0 ]; then
    echo "Error deleting stack. Exiting."
fi

echo "Waiting for stack to finish deleting."
aws cloudformation wait stack-delete-complete --profile p2r --stack-name p2r
if [ $? -ne 0 ]; then
    echo "Error waiting for stack to delete. Exiting."
fi

echo "Stack deleted."