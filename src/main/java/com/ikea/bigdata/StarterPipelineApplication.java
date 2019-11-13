package com.ikea.bigdata;

import com.ikea.bigdata.dataflow.pipeline.DataflowPipelineBuilder;

/**
 * This pipeline application lets you process data from a pubsub in protobuff format.
 * It applies series of transformations on the data and save it to the database.
 * It also emits processed data in CSV format.
 *
 * @version 0.1
 */
public class StarterPipelineApplication {

    public static void main(String[] args) {
        new DataflowPipelineBuilder().createDataPipeline(args).run();
    }
}
