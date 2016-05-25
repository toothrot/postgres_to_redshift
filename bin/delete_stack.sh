#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/../.env

aws cloudformation describe-stacks --profile p2r --stack-name p2r
if [ $? -ne 0 ]; then
    echo "Stack does not exist. Exiting."
exit 1
fi

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