#!/usr/bin/env bash

set -e

source hack/common.sh

(cd tools/cluster-deploy/ && go build)

CLUSTER_DEPLOY="tools/cluster-deploy/cluster-deploy"

$CLUSTER_DEPLOY --ocs-catalog-image="${FILE_BASED_CATALOG_FULL_IMAGE_NAME}" --ocs-subscription-channel="${OCS_SUBSCRIPTION_CHANNEL}" --install-namespace="${INSTALL_NAMESPACE}" --yaml-output-path=${DEPLOY_YAML_PATH}
