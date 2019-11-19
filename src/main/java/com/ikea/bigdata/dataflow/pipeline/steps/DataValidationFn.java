package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.protos.OrderProtos;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

@Slf4j
public class DataValidationFn extends DoFn<OrderProtos.Order, OrderProtos.Order> {
    @ProcessElement
    public void processElement(@Element OrderProtos.Order order, ProcessContext out) {
        if (Objects.nonNull(order)) {
            if (!StringUtils.isEmpty(order.getId())
                    && !StringUtils.isEmpty(order.getShippingaddress())
                    && !StringUtils.isEmpty(order.getEmail())
                    && (order.getCost() > 0 && order.getCost() < 8000)) {
                out.output(Constants.VALID_DATA, order);
            } else {
                log.error("Data received is corrupt. Sending it back to dead letter queue for reprocessing");
                System.out.println("Data received is corrupt. Sending it back to dead letter queue for reprocessing");
                out.output(Constants.INVALID_DATA, order);
            }
        }
    }
}
