package com.ikea.bigdata.util;

import com.ikea.bigdata.exception.FailureMetaData;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;

public class LogPipelineFailures implements Serializable {

    private static final String FAILURE_TEXT = "LogFailures";
    public static final TupleTag<FailureMetaData> FAILURE_TAG = new TupleTag<FailureMetaData>() {
    };



    // Log the pipeline failures on queue
    public static void logPipelineFailuresQueue(
            String outputTopic,
            final PCollectionTuple eventPayloadTuple) {

        eventPayloadTuple
                .get(FAILURE_TAG)
                .setCoder(SerializableCoder.of(FailureMetaData.class))
                .apply("Convert Event Payload Error to JSon", AsJsons.of(FailureMetaData.class))
                .apply(
                        FAILURE_TEXT.concat("<< YourDoFnHere >>"),
                        PubsubIO.writeStrings().to(outputTopic));
    }
}
