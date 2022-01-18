import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.transforms.JsonToRow;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public class MyPipeline {

    static final TupleTag<String> VALID_Messages = new TupleTag<String>() {
    };
    static final TupleTag<String> INVALID_Messages = new TupleTag<String>() {
    };

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    //required for building schema before passing data to table
    public static final Schema FinalSchema = Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();
    //setting options using DataflowPipelineOptions
    public interface MyOptions extends DataflowPipelineOptions, PipelineOptions {

        @Description("Input Topic name")
        void setInputSubscriptionName(String inputTopicName);
        String getInputSubscriptionName();

        @Description("Dead letter Topic name")
        void setDLQTopicName(String DLQTopicName);
        String getDLQTopicName();

    }

    //starting point
    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("nttdata-c4e-bde");

        TableReference tableSpec = new TableReference()
                .setProjectId("nttdata-c4e-bde")
                .setDatasetId("uc1_14")
                .setTableId("account");

        runPipeline(options, tableSpec);
    }
    //ParsingJson
    static class JsonToCommonLog extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String json, ProcessContext processContext) throws Exception {
            String[] arr = json.split(",");
            if (arr.length == 3) {
                if (arr[0].contains("id") && arr[1].contains("name") && arr[2].contains("surname")) {
                    processContext.output(VALID_Messages, json);
                } else {
                    processContext.output(INVALID_Messages, json);
                }
            } else {
                processContext.output(INVALID_Messages, json);
            }
        }
    }

    static public void runPipeline(MyOptions options, TableReference tableSpec) {
        Pipeline p = Pipeline.create(options);
        System.out.println("Creating Pipeline");

        PCollectionTuple pubSubMessages =
                p.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .fromSubscription(options.getInputSubscriptionName()))
                        .apply("ParseJson", ParDo.of(new JsonToCommonLog()).withOutputTags(VALID_Messages, TupleTagList.of(INVALID_Messages)));

        PCollection<String> parsedData = pubSubMessages.get(VALID_Messages);
        PCollection<String> unparsedData = pubSubMessages.get(INVALID_Messages);

        //happy path to BQtable
        parsedData.apply("TransformToRow", JsonToRow.withSchema(FinalSchema))
                .apply("WriteDataToTable", BigQueryIO.<Row>write().to(tableSpec).useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        //path to dlq
        unparsedData.apply("InvalidDataToDLQ", PubsubIO.writeStrings().to(options.getDLQTopicName()));

        p.run().waitUntilFinish();

    }
}

