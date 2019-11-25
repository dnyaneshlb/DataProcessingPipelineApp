package com.ikea.bigdata.dataflow.pipeline;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.dataflow.pipeline.options.DataPipelineOptions;
import com.ikea.bigdata.dataflow.pipeline.steps.DataValidationFn;
import com.ikea.bigdata.dataflow.pipeline.steps.ListConverterFn;
import com.ikea.bigdata.exception.DataPipelineException;
import com.ikea.bigdata.exception.FailureMetaData;
import com.ikea.bigdata.protos.OrderProtos;
import com.ikea.bigdata.util.CommonUtil;
import com.ikea.bigdata.util.LogPipelineFailures;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;

import java.io.Serializable;
import java.sql.PreparedStatement;
/*
    TODO :
           Exception Handling
           Use Merge statement instead of insert
 */
@Slf4j
public class DataflowPipelineBuilder implements Serializable {

    /**
     *  Create a pipeline and validate options.
     * @param args
     * @return a pipeline to run
     */
    public Pipeline createDataPipeline(String[] args) {
        log.debug("create data pipeline function is started");
        final DataPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPipelineOptions.class);

        //TODO : Do we need to this manual validation? can it be part of Pipeline options?
        final String projectName = options.getProject();
        if (StringUtils.isEmpty(projectName)) {
            log.error("Project is missing from pipeline options.");
            throw new DataPipelineException("Project is missing from pipeline options.");
        }

        // Create the Pipeline with the specified options
        final Pipeline pipeline = Pipeline.create(options);
        log.debug("Started processing events from pubsub");

        try {
            PCollection<OrderProtos.Order> events = readEvents(options, pipeline).setCoder(getCoder(pipeline));
            PCollection<Void> saveResults = save(options, events);
            emitToCSV(events, saveResults);
        } catch (Exception e) {
            log.error("Error in processing event. Message : {}", e.getMessage());
            //TODO : send failed message to new pubsub topic
            FailureMetaData failure = CommonUtil.getDataValidationFailureResponse(DataValidationFn.class.toString(),
                    "System Error", e.getMessage());
            //LogPipelineFailures.logPipelineFailuresQueue(options.getFailureDataTopic(), Create.OfValueProvider(failure, ));
        }
        return pipeline;
    }

    /**
     * Read pubsub events stream and apply windowing.
     * @param options pipeline options
     * @param pipeline a pipeline
     * @return a PCollection of <Code>Order<Code/> for next transformation.
     */
    private PCollection<OrderProtos.Order> readEvents(DataPipelineOptions options, Pipeline pipeline) {
        PCollection<OrderProtos.Order> events = null;
        try {
            events = pipeline
                    .apply("Read Pubsub Events", PubsubIO.readProtos(OrderProtos.Order.class)
                            .withIdAttribute(Constants.ATTRIBUTE_ID)
                            .fromSubscription(options.getEventSubscription()))
                    .apply(Window.<OrderProtos.Order>into(
                            FixedWindows.of(Duration.standardMinutes(options.getWindowSize())))
                            .triggering(
                                    Repeatedly.forever(
                                            AfterFirst.of(
                                                    AfterPane.elementCountAtLeast(options.getLeastElementsInWindow()),
                                                    AfterProcessingTime.pastFirstElementInPane()
                                                            .plusDelayOf(Duration.standardSeconds(10)))))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes());
        } catch (Exception e) {
            log.error("Error while reading events from pubsub subscription {}", options.getEventSubscription());
            throw new DataPipelineException("Exception while reading events from pub subscription " + options.getEventSubscription() +
                    "with message as " + e.getMessage());
        }
        return events;
    }

    /**
     * Responsible for writing events in CSV file.
     * @param events      streamed events
     * @param saveResults result of db save operation
     */
    private void emitToCSV(PCollection<OrderProtos.Order> events, PCollection<Void> saveResults) {
        log.debug("Emitting data in CSV format.");
        events/*.apply(Wait.on(saveResults))*/
                .apply("Convert to Comma Separated String", ParDo.of(ListConverterFn.builder().build()))
                .apply(TextIO
                        .write()
                        .withWindowedWrites()
                        .withHeader(Constants.CSV_MODEL_NUMBER
                                + Constants.SEPARATOR_COMMA + Constants.CSV_HEADER_SHIPPING_ADDRESS
                                + Constants.SEPARATOR_COMMA + Constants.CSV_HEADER_COST
                                + Constants.SEPARATOR_COMMA + Constants.CSV_HEADER_EMAIL
                                + Constants.SEPARATOR_COMMA + Constants.CSV_HEADER_MOBILE_NUMBER)
                        .withShardNameTemplate(Constants.NAME_TEMPLATE)
                        .to(Constants.FILE_NAME_PREFIX)
                        .withNumShards(Constants.NUM_OF_SHARDS)
                        .withSuffix(Constants.FILE_TYPE_SUFFIX)
                );
    }


    /**
     * Return appropriate coder required for decoding protobuf messages
     * @param pipeline data pipeline
     * @return a coder required to deserialize event payload
     */
    private Coder<OrderProtos.Order> getCoder(Pipeline pipeline) {
        // CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        Coder<OrderProtos.Order> coder = ProtoCoder.of(OrderProtos.Order.class).withExtensionsFrom(OrderProtos.class);
        //coderRegistry.registerCoderForClass(OrderProtos.Order.class, coder);
        return coder;
    }


    /**
     * Save valid event data into database.
     * Also process bad data.
     * @param options pipeline options
     * @param events  streamed events
     * @return a result of save operation on which we can wait.
     */
    private PCollection<Void> save(DataPipelineOptions options, PCollection<OrderProtos.Order> events) {
        log.debug("Saving event to database");
        try {
            PCollectionTuple taggedEvents = events
                    .apply(ParDo.of(new DataValidationFn()).withOutputTags(Constants.VALID_DATA,
                            TupleTagList.of(LogPipelineFailures.FAILURE_TAG)));
            PCollection<Void> dbWriteResults = taggedEvents
                    .get(Constants.VALID_DATA)
                    .apply("Save Event to Database",
                            JdbcIO.<OrderProtos.Order>write()
                                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                            Constants.POSTGRESS_DRIVER_CLASS, options.getDatabaseURL())
                                            .withUsername(options.getDatabaseUserName())
                                            .withPassword(options.getDatabasePassword()))
                                    .withStatement(Constants.INSERT_ORDER_QUERY)
                                    .withPreparedStatementSetter((OrderProtos.Order order, PreparedStatement query) -> {
                                        query.setInt(1, Integer.parseInt(order.getModelNumber()));
                                        query.setInt(2, order.getCost());
                                        query.setString(3, order.getEmail());
                                        query.setString(4, order.getShippingAddress());
                                        query.setString(5, order.getMobileNumber());
                                    })
                                    .withRetryStrategy(new JdbcIO.DefaultRetryStrategy())
                                    .withResults());

            LogPipelineFailures.logPipelineFailuresQueue(options.getFailureDataTopic(), taggedEvents.get(LogPipelineFailures.FAILURE_TAG));
            return dbWriteResults;
        } catch (Exception e) {
            log.error("Exception while saving data into database with message as {} and cause as {} ", e.getMessage(), e.getCause());
            throw new DataPipelineException("Exception while saving data into database with message as " + e.getMessage());
        }
    }
}
