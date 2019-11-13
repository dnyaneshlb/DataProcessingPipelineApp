package com.ikea.bigdata.dataflow.pipeline;

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

    final DataPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPipelineOptions.class);

    //TODO : Do we need to this manual validation? can it be part of Pipeline options?
    final String projectName = options.getProject();
    if (StringUtils.isEmpty(projectName)) {
      throw new DataPipelineException("Project is missing from pipeline options.");
    }

    // Create the Pipeline with the specified options
    final Pipeline pipeline = Pipeline.create(options);

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    Coder<OrderProtos.Order> coder = ProtoCoder.of(OrderProtos.Order.class).withExtensionsFrom(OrderProtos.class);
    coderRegistry.registerCoderForClass(OrderProtos.Order.class, coder);

    //transformations starts here
    PCollection<OrderProtos.Order> events = pipeline
            .apply("Read Pubsub Events", PubsubIO.readProtos(OrderProtos.Order.class)
                    .fromSubscription(options.getEventSubscription()))
            .setCoder(coder);

    events.apply("Save to Database",
            JdbcIO.<OrderProtos.Order>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                    "org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                    .withUsername("postgres")
                    .withPassword("postgres"))
                    .withStatement("insert into Orders values(?,?,?,?)")
                    .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<OrderProtos.Order>() {
                      @Override
                      public void setParameters(OrderProtos.Order order, PreparedStatement query) throws Exception {
                        query.setString(1,order.getId());
                        query.setString(2, order.getEmail());
                        query.setString(3, order.getShippingaddress());
                        query.setInt(4,order.getCost());
                      }
                    })
    );


    events.apply(Window.<OrderProtos.Order>into(
            FixedWindows.of(Duration.standardMinutes(2)))
            .triggering(
                    Repeatedly.forever(
                            AfterFirst.of(
                                    AfterPane.elementCountAtLeast(10),
                                    AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(Duration.standardSeconds(20)))))
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes())
    .apply("Convert to String", MapElements.via(new SimpleFunction<OrderProtos.Order, String>() {
        @Override
        public String apply(OrderProtos.Order order){
          StringBuilder orderString = new StringBuilder();
          orderString.append(order.getId())
                  .append(",")
                  .append(order.getShippingaddress())
                  .append(",")
                  .append(order.getCost())
                  .append(",")
                  .append(order.getEmail());
          return orderString.toString();
        }
    }))
    .apply(TextIO
            .write()
            .withWindowedWrites()
            .withHeader("id,email,Shipping Address,cost")
            .withShardNameTemplate("template")
            .to("orders")
            .withNumShards(1)
            .withSuffix(".csv")
    );


    log.debug("Data processed successfully.");
    return pipeline;
  }
}
