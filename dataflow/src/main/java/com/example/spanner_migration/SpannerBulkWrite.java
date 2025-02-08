/*
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
## How to run
mvn clean
mvn compile
mvn exec:java \
    -Dexec.mainClass=com.example.spanner_migration.SpannerBulkWrite \
    -Pdataflow-runner \
    -Dexec.args="--project=my-project-id \
                 --instanceId=my-instance-id \
                 --databaseId=my-database-id \
                 --table=my-table \
                 --serviceId={service_id} \
                 --importBucket=my-import-bucket \
                 --deadLetterBucket=my-dead-letter-bucket \
                 --runner=DataflowRunner \
                 --region=my-gcp-region"

Note: After a successful DynamoDB export to the import bucket,
the bucket should contain gzip JSON files.
*/

package com.example.spanner_migration;

import static org.apache.beam.sdk.io.Compression.GZIP;

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.Optional;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerBulkWrite {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerBulkWrite.class);

  public interface Options extends PipelineOptions {

    @Description("Service ID to write to")
    @Validation.Required
    String getServiceId();

    void setServiceId(String value);

    @Description("Spanner instance ID to write to")
    @Validation.Required
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Spanner database name to write to")
    @Validation.Required
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Spanner table name to write to")
    @Validation.Required
    String getTable();

    void setTable(String value);

    @Description("Location of your GCS Bucket with your exported database files")
    @Validation.Required
    String getImportBucket();

    void setImportBucket(String value);

    @Description("Location of your GCS Bucket for dead letter queue")
    @Validation.Required
    String getDeadLetterBucket();

    void setDeadLetterBucket(String value);
  }

  static final TupleTag<Record> validRecordsTag = new TupleTag<Record>(){};
  static final TupleTag<String> deadLetterTag = new TupleTag<String>(){};

  static class ParseRecords extends DoFn<String, Record> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        c.output(validRecordsTag, new Gson().fromJson(c.element(), Record.class));
      } catch (Exception e) {
        LOG.error("Error parsing record: " + c.element(), e);
        c.output(deadLetterTag, c.element());
      }
    }
  }

  static class CreateRecordMutations extends DoFn<Record, Mutation> {

    String table;
    String serviceId;

    public CreateRecordMutations(String table, String serviceId) {
      this.table = table;
      this.serviceId = serviceId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Record record = c.element();

      Mutation.WriteBuilder mutation = Mutation.newInsertOrUpdateBuilder(table);

      try {
        mutation.set("userId").to(record.Item.object_id.S);
        mutation.set("serviceId").to(serviceId);
        mutation.set("name").to(record.Item.attr_name.S);
        mutation.set("lastUpdate").to(record.Item.attr_time.N);
        Optional.ofNullable(record.Item.attr_string).ifPresent(x -> {
          mutation.set("stringValue").to(record.Item.attr_string.S);
        });
        Optional.ofNullable(record.Item.attr_value).ifPresent(x -> {
          mutation.set("integerValue").to(record.Item.attr_value.N);
        });

        c.output(mutation.build());

      } catch (Exception ex) {
        LOG.error("Unable to create mutation for record: " + record, ex);
        c.output(deadLetterTag, new Gson().toJson(record));
      }
    }
  }

  public static void main(String[] args) {
    System.out.println("args:" + Arrays.toString(args));
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    String inputFiles = "gs://" + options.getImportBucket() + "/" + options.getServiceId() + "/**/*.json.gz";
    String deadLetterFiles = "gs://" + options.getDeadLetterBucket() + "/" + options.getServiceId() + "/deadletters/list.json";

    PCollection<String> input = p.apply("ReadItems",
        TextIO.read().from(inputFiles).withCompression(GZIP));

    PCollectionTuple parsedRecords = input.apply("ParseRecords", ParDo.of(new ParseRecords())
        .withOutputTags(validRecordsTag, TupleTagList.of(deadLetterTag)));

    PCollection<Record> validRecords = parsedRecords.get(validRecordsTag);
    PCollection<String> deadLetters = parsedRecords.get(deadLetterTag);

    PCollection<Mutation> mutations = validRecords.apply("CreateRecordMutations",
        ParDo.of(new CreateRecordMutations(options.getTable(), options.getServiceId())));

    mutations.apply("WriteRecords", SpannerIO.write()
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId()));

    deadLetters.apply("WriteDeadLetters", TextIO.write().to(deadLetterFiles).withSuffix(".json"));

    p.run().waitUntilFinish();
  }

  public static class Record implements Serializable {
    private Item Item;
  }

  public static class Item implements Serializable {
    private ObjectId object_id;
    private AttrName attr_name;
    private AttrTime attr_time;
    private AttrString attr_string;
    private AttrValue attr_value;
  }

  public static class ObjectId implements Serializable {
    private String S;
  }

  public static class AttrName implements Serializable {
    private String S;
  }

  public static class AttrString implements Serializable {
    private String S;
  }

  public static class AttrTime implements Serializable {
    private String N;
  }

  public static class AttrValue implements Serializable {
    private String N;
  }
}
