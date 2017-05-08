#!/usr/bin/env bash

aws cloudformation describe-stacks --profile p2r --stack-name p2r > /dev/null
if [ $? -ne 0 ]; then
    echo "Stack does not exist. Exiting."
exit 1
fi

export P2RS_S3_EXPORT_BUCKET=$(aws cloudformation describe-stack-resources --profile p2r --stack-name p2r --logical-resource-id s3Bucket | jq -r ".StackResources[0].PhysicalResourceId")
P2R_REDSHIFT_ID=$(aws cloudformation describe-stack-resources --profile p2r --stack-name p2r --logical-resource-id Redshift | jq -r ".StackResources[0].PhysicalResourceId")
P2R_REDSHIFT_ADDRESS=$(aws redshift describe-clusters --profile p2r --cluster-identifier ${P2R_REDSHIFT_ID} | jq -r ".Clusters[0].Endpoint.Address")
export P2RS_TARGET_URI="postgres://test:Testtesttest1@${P2R_REDSHIFT_ADDRESS}:5439/test"

echo "P2RS_S3_EXPORT_BUCKET=${P2R_S3_BUCKET}"
echo "P2RS_TARGET_URI=${P2RS_TARGET_URI}"
