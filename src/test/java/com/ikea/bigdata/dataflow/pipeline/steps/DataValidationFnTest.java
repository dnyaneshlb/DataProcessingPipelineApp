package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.protos.OrderProtos.Order;
import org.apache.beam.sdk.transforms.DoFn;

public class DataValidationFnTest extends DoFn<Order,Order> {
    DataValidationFn dataValidationFn = new DataValidationFn();
}
