package com.ikea.bigdata.util;

import com.ikea.bigdata.exception.FailureMetaData;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;
import java.time.Instant;

public class LogPipelineFailures implements Serializable {

    private static final String FAILURE_TEXT = "Log Failures ";
    public static final TupleTag<FailureMetaData> FAILURE_TAG = new TupleTag<FailureMetaData>() {
    };

    // Log the pipeline failures on queue
    public static void logPipelineFailuresQueue(String outputTopic, final PCollection<FailureMetaData> failureTuple) {
        failureTuple
                .setCoder(SerializableCoder.of(FailureMetaData.class))
                .apply("Convert Event Payload Error to JSon", AsJsons.of(FailureMetaData.class))
                .apply(FAILURE_TEXT.concat("of data validation to pubsub"),
                        PubsubIO.writeStrings()
                                .to(outputTopic)
                                .withIdAttribute("validation-failed-orders")
                                .withTimestampAttribute(String.valueOf(Instant.now().toEpochMilli()))
                );
    }
}
