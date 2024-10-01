include .env
export $(shell sed 's/=.*//' .env)

ifeq ($(GCP_PROJECT_ID), sprocket-evaluation2)
  DYNAMO_DB_TABLE_NAME := v2-sprocket-${SERVICE_ID}_game_table
  SPANNER_INSTANCE_ID := tfgen-spanid-20240522052017533
else
  DYNAMO_DB_TABLE_NAME := stg-v2-sprocket-${SERVICE_ID}_game_table
  SPANNER_INSTANCE_ID := tfgen-spanid-20240514075746935
endif
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)

.PHONY: export-dynamodb-to-s3
export-dynamodb-to-s3:
	gcloud pubsub topics create spanner-migration-${SERVICE_ID}
	gcloud pubsub subscriptions create spanner-migration-${SERVICE_ID} \
    --topic spanner-migration-${SERVICE_ID}
	AWS_PAGER='' aws dynamodb update-continuous-backups \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
	AWS_PAGER='' aws dynamodb update-table \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
	STREAMARN=$$(aws dynamodb describe-table \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--query "Table.LatestStreamArn" \
		--output text) &&\
	AWS_PAGER='' aws lambda create-event-source-mapping \
		--event-source-arn $${STREAMARN} \
		--function-name dynamodb-spanner-lambda \
		--enabled \
		--starting-position TRIM_HORIZON
	aws dynamodb export-table-to-point-in-time \
		--table-arn arn:aws:dynamodb:ap-northeast-1:${AWS_ACCOUNT_ID}:table/${DYNAMO_DB_TABLE_NAME} \
		--s3-bucket ${S3_BUCKET_NAME} \
		--s3-prefix ${SERVICE_ID} \
		--export-format DYNAMODB_JSON

.PHONY: transfer-to-gcs
transfer-to-gcs:
	aws dynamodb update-continuous-backups \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--point-in-time-recovery-specification PointInTimeRecoveryEnabled=false
	echo '{ "roleArn": "arn:aws:iam::$(AWS_ACCOUNT_ID):role/assume_role_for_spanner_migration" }' > credential.json
	gcloud transfer jobs create \
		s3://${S3_BUCKET_NAME}/${SERVICE_ID}/ \
		gs://${GCS_BUCKET_NAME}/${SERVICE_ID}/\
		--source-creds-file credential.json \
		--format="value(name)"

.PHONY: bulk-write
bulk-write:
	cd dataflow && \
	mvn compile && \
	mvn exec:java \
	-Dexec.mainClass=com.example.spanner_migration.SpannerBulkWrite \
	-Pdataflow-runner \
	-Dexec.args="--project=${GCP_PROJECT_ID} \
	--instanceId=${SPANNER_INSTANCE_ID} \
	--databaseId=user_state \
	--table=states \
	--serviceId=${SERVICE_ID} \
	--importBucket=${GCS_BUCKET_NAME} \
	--deadLetterBucket=${GCS_DLQ_BUCKET} \
	--runner=DataflowRunner \
	--region=asia-northeast1 \
	--maxNumWorkers=${MAX_NUM_WORKERS} \
	--jobName=spanner-bulk-write-${SERVICE_ID} \
	--labels='{\"datadog-disabled\": \"true\"}'" &

.PHONY: streaming-write
streaming-write:
	cd dataflow &&\
	mvn compile &&\
	mvn exec:java \
	-Dexec.mainClass=com.example.spanner_migration.SpannerStreamingWrite \
	-Pdataflow-runner \
	-Dexec.args="--project=${GCP_PROJECT_ID} \
	--subscription=projects/${GCP_PROJECT_ID}/subscriptions/spanner-migration-${SERVICE_ID} \
	--instanceId=${SPANNER_INSTANCE_ID} \
	--databaseId=user_state \
	--serviceId=${SERVICE_ID} \
	--table=states \
	--deadLetterBucket=${GCS_DLQ_BUCKET} \
	--experiments=allow_non_updatable_job \
	--runner=DataflowRunner \
	--region=asia-northeast1 \
	--maxNumWorkers=${MAX_NUM_WORKERS} \
	--jobName=spanner-streaming-write-${SERVICE_ID} \
	--labels='{\"datadog-disabled\": \"true\"}'" &

.PHONY: drain-job
drain-job:
	UUID=$$(aws lambda list-event-source-mappings | jq -r '.EventSourceMappings[] | \
	 select(.EventSourceArn | contains("${DYNAMO_DB_TABLE_NAME}")) | .UUID') &&\
	AWS_PAGER='' aws lambda delete-event-source-mapping --uuid $${UUID}
	AWS_PAGER='' aws dynamodb update-table \
    --table-name ${DYNAMO_DB_TABLE_NAME} \
    --stream-specification StreamEnabled=false
	JOB_ID=$$(gcloud dataflow jobs list --status=active \
	--filter="name=spanner-streaming-write-${SERVICE_ID}" --format="value(JOB_ID)" --region="asia-northeast1") &&\
	gcloud dataflow jobs drain $${JOB_ID} --region="asia-northeast1"

.PHONY: close-pubsub
close-pubsub:
	gcloud pubsub topics delete spanner-migration-${SERVICE_ID}
	gcloud pubsub subscriptions delete spanner-migration-${SERVICE_ID}
