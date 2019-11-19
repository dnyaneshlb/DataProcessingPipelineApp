package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.protos.OrderProtos;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
@Builder
public class ListConverterFn extends DoFn<OrderProtos.Order, String> {
    @ProcessElement
    public void processElement(@Element OrderProtos.Order order, OutputReceiver<String> out) {
        log.debug("Creating comma separated list of order attributes");
        StringBuilder orderString = new StringBuilder();
        orderString.append(order.getId())
                .append(Constants.SEPARATOR_COMMA)
                .append(order.getShippingaddress())
                .append(Constants.SEPARATOR_COMMA)
                .append(order.getCost())
                .append(Constants.SEPARATOR_COMMA)
                .append(order.getEmail());
        out.output(orderString.toString());
    }
}
