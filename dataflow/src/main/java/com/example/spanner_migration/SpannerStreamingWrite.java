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
    -Dexec.mainClass=com.example.spanner_migration.SpannerStreamingWrite \
    -Pdataflow-runner \
    -Dexec.args="--project=$GOOGLE_CLOUD_PROJECT \
                 --instanceId=my-instance-id \
                 --databaseId=my-database-id \
                 --table=my-table \
                 --experiments=allow_non_updatable_job \
                 --subscription=my-pubsub-subscription
                 --runner=DataflowRunner \
                 --region=my-gcp-region"
*/

package com.example.spanner_migration;

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;


public class SpannerStreamingWrite {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStreamingWrite.class);
  static final TupleTag<Item> validTag = new TupleTag<Item>(){};
  static final TupleTag<String> invalidTag = new TupleTag<String>(){};

  public interface Options extends PipelineOptions {

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

    @Description("Service ID to write to")
    @Validation.Required
    String getServiceId();

    void setServiceId(String value);

    @Description("Pub/Sub Subscription with streaming changes (full subscription path reqd")
    @Validation.Required
    String getSubscription();

    void setSubscription(String value);

    @Description("How often to process the window (s)")
    @Default.String("10")
    String getWindow();

    void setWindow(String value);

    @Description("GCS bucket for dead letters")
    @Validation.Required
    String getDeadLetterBucket();

    void setDeadLetterBucket(String value);
  }

  static class CreateUpdateItems extends DoFn<String, Item> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        JsonObject json = JsonParser.parseString(c.element()).getAsJsonObject();
        if (json.has("NewImage")) {
          LOG.warn("received a create/update");
          LOG.warn(json.getAsJsonObject("NewImage").toString());
          LOG.warn(validTag.toString());
          c.output(validTag, new Gson().fromJson(json.getAsJsonObject("NewImage"), Item.class));
        }
      } catch (Exception e) {
        LOG.error("Error parsing record: " + c.element(), e);
        c.output(invalidTag, c.element());
      }
    }
  }

  static class DeleteItems extends DoFn<String, Item> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        JsonObject json = JsonParser.parseString(c.element()).getAsJsonObject();
        if (!json.has("NewImage")) {
          LOG.warn("received a delete");
          LOG.warn(json.getAsJsonObject("Keys").toString());
          LOG.warn(validTag.toString());
          c.output(validTag, new Gson().fromJson(json.getAsJsonObject("Keys"), Item.class));
        }
      } catch (Exception e) {
        LOG.error("Error parsing record: " + c.element(), e);
        c.output(invalidTag, c.element());
      }
    }
  }

  static class UpdateMutations extends DoFn<Item, Mutation> {

    String table;
    String serviceId;

    public UpdateMutations(String table,String serviceId) {
      this.table = table;
      this.serviceId = serviceId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Item item = c.element();
      Mutation.WriteBuilder mutation = Mutation.newReplaceBuilder(table);

      try {
        mutation.set("userId").to(item.object_id.S);
        mutation.set("serviceId").to(serviceId);
        mutation.set("name").to(item.attr_name.S);
        mutation.set("lastUpdate").to(item.attr_time.N);

        Optional.ofNullable(item.attr_string).ifPresent(x -> {
          mutation.set("stringValue").to(item.attr_string.S);
        });
        Optional.ofNullable(item.attr_value).ifPresent(x -> {
          mutation.set("integerValue").to(item.attr_value.N);
        });

        // mutation.set("userId").to(record.Item.object_id.S);
        // mutation.set("serviceId").to(serviceId);
        // mutation.set("name").to(record.Item.attr_name.S);
        // mutation.set("lastUpdate").to(record.Item.attr_time.N);
        // Optional.ofNullable(record.Item.attr_string).ifPresent(x -> {
        //   mutation.set("stringValue").to(record.Item.attr_string.S);
        // });
        // Optional.ofNullable(record.Item.attr_value).ifPresent(x -> {
        //   mutation.set("integerValue").to(record.Item.attr_value.N);
        // });

        c.output(mutation.build());

      } catch (Exception ex) {
        LOG.error("Unable to create mutation for record: " + item, ex);
        c.output(invalidTag, new Gson().toJson(item));
      }
    }
  }

  static class DeleteMutations extends DoFn<Item, Mutation> {

    String table;
    String serviceId;

    public DeleteMutations(String table,String serviceId) {
      this.table = table;
      this.serviceId = serviceId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        Item item = c.element();
        Mutation mutation = Mutation.delete(table, com.google.cloud.spanner.Key.of(serviceId,item.object_id.S,item.attr_name.S));
        c.output(mutation);
      } catch (Exception e) {
        LOG.error("Error parsing record: " + c.element(), e);
        c.output(invalidTag, c.element().toString());
      }
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    PCollection<String> messages = p.apply("Reading from PubSub",
        PubsubIO.readStrings().fromSubscription(options.getSubscription()));

    PCollectionTuple parsedCreateUpdate = messages.apply("Create-or-Update?",
        ParDo.of(new CreateUpdateItems()).withOutputTags(validTag, TupleTagList.of(invalidTag)));

    PCollectionTuple parsedDelete = messages.apply("Delete?",
        ParDo.of(new DeleteItems()).withOutputTags(validTag, TupleTagList.of(invalidTag)));

    PCollection<Item> validCreateUpdateRecords = parsedCreateUpdate.get(validTag);
    PCollection<Item> validDeleteRecords = parsedDelete.get(validTag);
    PCollection<String> deadLetters = PCollectionList.of(parsedCreateUpdate.get(invalidTag))
        .and(parsedDelete.get(invalidTag))
        .apply(Flatten.pCollections());

    PCollection<Mutation> updates = validCreateUpdateRecords.apply("CU->Mutations",
        ParDo.of(new UpdateMutations(options.getTable(),options.getServiceId())));

    PCollection<Mutation> deletes = validDeleteRecords.apply("D->Mutations",
        ParDo.of(new DeleteMutations(options.getTable(),options.getServiceId())));

    PCollectionList<Mutation> merged = PCollectionList.of(updates).and(deletes);

    PCollection<Mutation> mergedWindowed = merged.apply("Merging Mutations",
            Flatten.pCollections())
        .apply("Creating Windows", Window
            .<Mutation>into(
                FixedWindows.of(Duration.standardSeconds(Long.parseLong(options.getWindow()))))
            .triggering(
                AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(10)))
            .withAllowedLateness(Duration.standardMinutes(300)).discardingFiredPanes());

    mergedWindowed.apply("Commit->Spanner",
        SpannerIO.write().withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId()).withBatchSizeBytes(0));

    String deadLetterFiles = "gs://" + options.getDeadLetterBucket() + "/stream/deadletters/list.json";
    deadLetters
    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
    .apply("WriteDeadLetters", TextIO.write()
        .to(deadLetterFiles)
        .withWindowedWrites()
        .withNumShards(1)
        .withSuffix(".json"));
    p.run();
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
