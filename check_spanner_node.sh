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
QUERY_GETKEY="SELECT id FROM service_key WHERE service_id='${SERVICE_ID}' AND active=1 LIMIT 1;"

echo "Waiting core-engine execute logs for ${SERVICE_ID}..."
IP=$(ssh $BASTION "spt ls Engine_spanner | cut -f2" 2>/dev/null)
while true; do
  set +e
  LOG=""
  for i in $IP; do
    LOG="$LOG$(ssh $BASTION "ssh $i 'grep ${SERVICE_ID} /var/log/core-engine/execute.log'" 2>/dev/null)"
  done
  set -e
  if [ -n "$LOG" ]; then
    echo "$LOG" | tail -n 5 | jq -cC .
    break
  fi
  echo "  Wait 5 secs..."
  sleep 5
done

USER_ID=$(echo "$LOG" | tail -n 1 | jq -r '.user_id')
COUNTER=$(echo "$LOG" | tail -n 1 | jq -r '."state.name"')
echo "Checking counter access for user ${USER_ID}..."
API_KEY=$(docker run --rm mysql:8.0 mysql \
  --default-character-set=utf8mb4 \
  -h host.docker.internal \
  -P 13306 \
  -u app -p"$MYSQL_PASS" sprocket \
  -B -N -e "$QUERY_GETKEY" 2>/dev/null)

echo "  curl -s https://${API_HOST}/services/${SERVICE_ID}/keys/${API_KEY}/users/${USER_ID}/counters/${COUNTER} | jq -cC ."
curl -s https://${API_HOST}/services/${SERVICE_ID}/keys/${API_KEY}/users/${USER_ID}/counters/${COUNTER} | jq -cC .
read -p "Do you want to continue? [y/N]: " ANSWER
case $ANSWER in
[Yy]*) ;;
*)
  exit 1
  ;;
esac
