include .env
export $(shell sed 's/=.*//' .env)

ifeq ($(GCP_PROJECT_ID), sprocket)
  DYNAMO_DB_TABLE_NAME := v2-sprocket-${SERVICE_ID}_game_table
else
  DYNAMO_DB_TABLE_NAME := stg-v2-sprocket-${SERVICE_ID}_game_table
endif
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)

.PHONY: export-dynamodb-to-s3
export-dynamodb-to-s3:
	aws dynamodb update-continuous-backups \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
	aws dynamodb update-table \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
	STREAMARN=$$(aws dynamodb describe-table \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--query "Table.LatestStreamArn" \
		--output text) &&\
	aws lambda create-event-source-mapping \
		--event-source-arn $${STREAMARN} \
		--function-name dynamodb-spanner-lambda \
		--enabled \
		--starting-position TRIM_HORIZON
	aws dynamodb export-table-to-point-in-time \
		--table-arn arn:aws:dynamodb:ap-northeast-1:${AWS_ACCOUNT_ID}:table/${DYNAMO_DB_TABLE_NAME} \
		--s3-bucket ${S3_BUCKET_NAME} \
		--s3-prefix ${SERVICE_ID} \
		--export-format DYNAMODB_JSON

.PHONY: reset-dynamodb-settings
reset-dynamodb-settings:
	aws dynamodb update-table \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--stream-specification StreamEnabled=false
	aws dynamodb update-continuous-backups \
		--table-name ${DYNAMO_DB_TABLE_NAME} \
		--point-in-time-recovery-specification PointInTimeRecoveryEnabled=false

.PHONY: transfer-to-gcs
transfer-to-gcs:
	echo '{ "roleArn": "arn:aws:iam::$(AWS_ACCOUNT_ID):role/assume_role_for_sprocket_poc" }' > credential.json
	JOB_NAME=$$(gcloud transfer jobs create \
		s3://${S3_BUCKET_NAME}/${SERVICE_ID}/ \
		gs://${GCS_BUCKET_NAME}/${SERVICE_ID}/\
		--source-creds-file credential.json \
		--format="value(name)")
	gcloud transfer jobs run ${JOB_NAME} 

.PHONY: bulk-write
bulk-write:
	cd dataflow &&\
	mvn compile &&\
	mvn exec:java \
		-Dexec.mainClass=com.example.spanner_migration.SpannerBulkWrite \
		-Pdataflow-runner \
		-Dexec.args="--project=${GCP_PROJECT_ID} \
					--instanceId=user_state \
					--databaseId=user_state \
					--table=states \
					--serviceId=${SERVICE_ID} \
					--importBucket=${GCS_BUCKET_NAME} \
					--deadLetterBucket=${GCS_DLQ_BUCKET} \
					--runner=DataflowRunner \
					--region=asia-northeast1 \
					--maxNumWorkers=${MAX_NUM_WORKERS}"

.PHONY: streaming-write
streaming-write:
	cd dataflow &&\
	mvn compile &&\
	mvn exec:java \
		-Dexec.mainClass=com.example.spanner_migration.SpannerStreamingWrite \
		-Pdataflow-runner \
		-Dexec.args="--project=${GCP_PROJECT_ID} \
					--subscription=projects/${GCP_PROJECT_ID}/subscriptions/spanner-migration \
					--instanceId=user_states \
					--databaseId=user_states \
					--table=states \
					--deadLetterBucket=${GCS_DLQ_BUCKET} \
					--experiments=allow_non_updatable_job \
					--runner=DataflowRunner \
					--region=asia-northeast1 \
					--maxNumWorkers=${MAX_NUM_WORKERS}"
