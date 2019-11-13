package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.protos.OrderProtos;
import org.apache.beam.sdk.transforms.DoFn;

public class ListConverterFn extends DoFn<OrderProtos.Order, String> {
    @ProcessElement
    public void processElement(@Element OrderProtos.Order order, OutputReceiver<String> out){
        System.out.println("processing order with mail id as "+ order.getEmail());
        StringBuilder orderString = new StringBuilder();
        orderString.append(order.getId())
                .append(order.getShippingaddress())
                .append(order.getCost())
                .append(order.getEmail()).toString();
        out.output(orderString.toString());
    }
}
