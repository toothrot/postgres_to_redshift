#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

S3_DATABASE_EXPORT_ID=""
S3_DATABASE_EXPORT_KEY=""

source ${DIR}/../.env

if [ ${S3_DATABASE_EXPORT_ID} = "" ]; then
    exit 1
fi

if [ ${S3_DATABASE_EXPORT_KEY} = "" ]; then
    exit 1
fi

aws configure set aws_access_key_id ${S3_DATABASE_EXPORT_ID} --profile p2r
aws configure set aws_secret_access_key ${S3_DATABASE_EXPORT_KEY} --profile p2r
aws configure set default.region us-east-1 --profile p2r
