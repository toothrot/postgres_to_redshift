#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

P2RS_S3_EXPORT_ID=""
P2RS_S3_EXPORT_KEY=""

source ${DIR}/../.env

if [ ${P2RS_S3_EXPORT_ID} = "" ]; then
    exit 1
fi

if [ ${P2RS_S3_EXPORT_KEY} = "" ]; then
    exit 1
fi

aws configure set aws_access_key_id $${P2RS_S3_EXPORT_ID} --profile p2r
aws configure set aws_secret_access_key ${P2RS_S3_EXPORT_KEY} --profile p2r
aws configure set default.region us-east-1 --profile p2r
