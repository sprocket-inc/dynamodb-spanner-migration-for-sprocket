#!/bin/bash
set -eu

BASTION=bastion
API_HOST=api.v2.sprocket.bz
SERVICE_ID=$1

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <service_id>"
  exit 1
fi

shopt -s expand_aliases
alias jq='docker run -i --rm --net=none jetbrainsinfra/jq jq'
MYSQL_PASS=$(aws ssm get-parameter --name "/core-database/mysql/app/password" --with-decryption --query "Parameter.Value" --output text)
QUERY_SELECT="SELECT id,name,node_id,active,last_update,created_time FROM service_info WHERE id='${SERVICE_ID}'\\G"
QUERY_UPDATE="UPDATE service_info SET node_id='spanner', last_update=NOW() WHERE id='${SERVICE_ID}' AND node_id!='spanner';"

echo "Changing service node to spenner node..."
docker run --rm mysql:8.0 mysql \
  --default-character-set=utf8mb4 \
  -h host.docker.internal \
  -P 13306 \
  -u app -p"${MYSQL_PASS}" sprocket \
  -e "${QUERY_UPDATE} ${QUERY_SELECT}" 2>/dev/null

echo "Invoking DumpServiceInfo Lambda..."
AWS_PAGER='' aws lambda invoke --function-name DumpServiceInfoStack-production-Lambda output.txt
