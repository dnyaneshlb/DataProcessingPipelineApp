package com.ikea.bigdata.dataflow.pipeline;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.dataflow.pipeline.options.DataPipelineOptions;
import com.ikea.bigdata.exception.DataPipelineException;
import com.ikea.bigdata.protos.OrderProtos;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;

import java.io.Serializable;
import java.sql.PreparedStatement;

/*
    TODO : Externalise parameters
           Code refactoring
           Exception Handling
           Error Tuple processing
           CSV overwrite issue
           Downgrade beam sdk
 */
@Slf4j
public class DataflowPipelineBuilder implements Serializable {

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

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    Coder<OrderProtos.Order> coder = ProtoCoder.of(OrderProtos.Order.class).withExtensionsFrom(OrderProtos.class);
    coderRegistry.registerCoderForClass(OrderProtos.Order.class, coder);

    //transformations starts here
    log.debug("Started processing events from pubsub");
    PCollection<OrderProtos.Order> events = pipeline
            .apply("Read Pubsub Events", PubsubIO.readProtos(OrderProtos.Order.class)
                    .fromSubscription(options.getEventSubscription()))
            .setCoder(coder);

    log.debug("Saving event to database");
    events.apply("Save Event to Database",
            JdbcIO.<OrderProtos.Order>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                    Constants.POSTGRESS_DRIVER_CLASS, options.getDatabaseURL())
                    .withUsername(options.getDatabaseUserName())
                    .withPassword(options.getDatabasePassword()))
                    .withStatement(Constants.INSERT_ORDER_QUERY)
                    .withPreparedStatementSetter((OrderProtos.Order order, PreparedStatement query) -> {
                        query.setInt(1, Integer.parseInt(order.getId()));
                        query.setInt(2,order.getCost());
                        query.setString(3, order.getEmail());
                        query.setString(4, order.getShippingaddress());
                    }));


    log.debug("Emitting data in CSV format.");
    events.apply(Window.<OrderProtos.Order>into(
            FixedWindows.of(Duration.standardMinutes(options.getWindowSize())))
            .triggering(
                    Repeatedly.forever(
                            AfterFirst.of(
                                    AfterPane.elementCountAtLeast(10),
                                    AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(Duration.standardSeconds(options.getLeastElementsInWindow())))))
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes())
    .apply("Convert to String", MapElements.via(new SimpleFunction<OrderProtos.Order, String>() {
        @Override
        public String apply(OrderProtos.Order order){
          StringBuilder orderString = new StringBuilder();
          orderString.append(order.getId())
                  .append(Constants.SEPERATOR_COMMA)
                  .append(order.getShippingaddress())
                  .append(Constants.SEPERATOR_COMMA)
                  .append(order.getCost())
                  .append(Constants.SEPERATOR_COMMA)
                  .append(order.getEmail());
          return orderString.toString();
        }
    }))
    .apply(TextIO
            .write()
            .withWindowedWrites()
            .withHeader(Constants.CSV_HEADER_ID + Constants.CSV_HEADER_EMAIL
                    + Constants.CSV_HEADER_SHIPPING_ADDRESS + Constants.CSV_HEADER_COST)
            .withShardNameTemplate(Constants.NAME_TEMPLATE)
            .to(Constants.FILE_NAME_SUFFIX)
            .withNumShards(Constants.NUM_OF_SHARDS)
            .withSuffix(Constants.FILE_TYPE_SUFFIX)
    );


    log.debug("Data processed successfully.");
    return pipeline;
  }
}
