package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.protos.OrderProtos;
import com.ikea.bigdata.util.CommonUtil;
import com.ikea.bigdata.util.LogPipelineFailures;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

@Slf4j
public class DataValidationFn extends DoFn<OrderProtos.Order, OrderProtos.Order> {
    /*
     * Counter tracking the number of output Pub/Sub messages after validation has failed.
     */
    private static final Counter ERROR_COUNTER = Metrics.counter(DataValidationFn.class, "validation-failed-messages");


    @ProcessElement
    public void processElement(@Element OrderProtos.Order order, ProcessContext out) {
        if (Objects.nonNull(order)) {
            if (!StringUtils.isEmpty(order.getModelNumber())
                    && !StringUtils.isEmpty(order.getShippingAddress())
                    && !StringUtils.isEmpty(order.getEmail())
                    && (order.getCost() > 0 && order.getCost() < 8000)
                    && order.getMobileNumber().length() == 10) {
                out.output(Constants.VALID_DATA, order);
            } else {
                ERROR_COUNTER.inc();
                log.error("Data received is corrupt. Sending it back to dead letter queue for reprocessing");
                out.output(LogPipelineFailures.FAILURE_TAG,
                        CommonUtil.getDataValidationFailureResponse(DataValidationFn.class.toString(),
                                "Business Validation Failed", order.toString()));
            }
        }
    }
}
