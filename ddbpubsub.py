#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import re
import site
import os
import json
import base64
from google.oauth2 import service_account

site.addsitedir(os.getcwd())

from google.cloud import pubsub_v1

print('Loading function')

def extract_service_id(input_string):
    pattern = r'[a-f0-9]{32}'
    match = re.search(pattern, input_string)
    
    if match:
        return match.group(0)
    else:
        return None

def lambda_handler(event, context):
    credsjson = json.loads(base64.b64decode(os.environ['SVCACCT']))
    credentials = service_account.Credentials.from_service_account_info(credsjson)
    scoped_creds = credentials.with_scopes(['https://www.googleapis.com/auth/pubsub'])
    publisher = pubsub_v1.PublisherClient(credentials=scoped_creds)
    
    #process incoming event
    for record in event['Records']:
        print(record['eventID'])
        print(record['eventName'])
        print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
        event_source_arn = record['eventSourceARN']
        table_name = event_source_arn.split('/')[1]
        service_id = extract_service_id(table_name)
        if service_id is None:
            print("failed to get service id")
            return
        
        topic_name = os.environ['TOPIC'] + "-" + service_id
        topic_path = publisher.topic_path(os.environ['PROJECT'], topic_name)

        push_record = record['dynamodb']
        future = publisher.publish(topic_path, data=json.dumps(push_record).encode("utf-8"))
        print("Pub/Sub message_id: %s" % future.result())
    return 'Successfully processed {} records.'.format(len(event['Records']))
