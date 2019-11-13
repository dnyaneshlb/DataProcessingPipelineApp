package com.ikea.bigdata.dataflow.pipeline.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface DataPipelineOptions extends GcpOptions {

    @Validation.Required
    @Description("Topic to read payload from ingestion queue")
    String getOrderEventsTopic();

    void setOrderEventsTopic(String orderEventsTopic);


    @Validation.Required
    @Description("Subscription from which payload is read")
    String getEventSubscription();

    void setEventSubscription(String eventSubscription);

    @Validation.Required
    @Description("Database URL command line argument.")
    String getDatabaseURL();

    void setDatabaseURL(String databaseURL);

    @Validation.Required
    @Description("Database USERNAME command line argument.")
    String getDatabaseUserName();

    void setDatabaseUserName(String databaseUserName);

    @Validation.Required
    @Description("Database PASSWORD command line argument.")
    String getDatabasePassword();

    void setDatabasePassword(String databasePassword);

    @Description("Failure Log Topic")
    String getFailureDataTopic();

    void setFailureDataTopic(String topic);
}
