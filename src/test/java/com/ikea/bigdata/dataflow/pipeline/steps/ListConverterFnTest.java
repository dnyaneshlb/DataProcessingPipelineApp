package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.common.Constants;
import com.ikea.bigdata.protos.OrderProtos;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListConverterFnTest {
    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testConvertListToString(){
        List<OrderProtos.Order> validOrders = getValidOrders();
        PCollection<String> result = pipeline.apply("Create", Create.of(validOrders))
                .apply("Conversion", ParDo.of(new ListConverterFn()));
        OrderProtos.Order order = validOrders.get(0);
        Assert.assertNotNull(result);
        PAssert.that(result).containsInAnyOrder(Arrays.asList(
                order.getModelNumber() + Constants.SEPARATOR_COMMA
                + order.getShippingAddress() + Constants.SEPARATOR_COMMA
                + order.getCost() + Constants.SEPARATOR_COMMA
                + order.getEmail() + Constants.SEPARATOR_COMMA
                + order.getMobileNumber()));
        pipeline.run();
    }


    private List<OrderProtos.Order> getValidOrders() {
        OrderProtos.Order order1 = OrderProtos.Order.newBuilder()
                .setCost(100)
                .setModelNumber("1234")
                .setMobileNumber("2321312321")
                .setEmail("dnyaesh@gmail.com")
                .setShippingAddress("asaS SSSAS asSAsa")
                .build();
        List<OrderProtos.Order> list = new ArrayList<>();
        list.add(order1);
        return list;
    }
}
