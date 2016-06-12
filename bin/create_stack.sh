#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/../.env

aws cloudformation describe-stacks --profile p2r --stack-name p2r
EXISTS=$?

if (( EXISTS == 0 )); then
    echo "Stack already exists. Exiting."
exit 1
fi

aws cloudformation create-stack --profile p2r --stack-name p2r --template-body file:///${DIR}/../config/cloud-formation-local-postgres.json --on-failure DELETE
if [ $? -ne 0 ]; then
    echo "Error creating stack. Exiting."
fi

echo "Waiting for stack to finish creating."
aws cloudformation wait stack-create-complete --profile p2r --stack-name p2r