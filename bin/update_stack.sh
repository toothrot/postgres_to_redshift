#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/../.env

aws cloudformation describe-stacks --profile p2r --stack-name p2r
if [ $? -ne 0 ]; then
    echo "Stack does not exist. Exiting."
exit 1
fi

aws cloudformation update-stack --profile p2r --stack-name p2r --template-body file:///${DIR}/../config/cloud-formation-local-postgres.json --on-failure DELETE
if [ $? -ne 0 ]; then
    echo "Error updating stack. Exiting."
fi

echo "Waiting for stack to finish updating."
aws cloudformation wait stack-update-complete --profile p2r --stack-name p2r