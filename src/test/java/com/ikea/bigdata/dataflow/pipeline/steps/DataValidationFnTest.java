package com.ikea.bigdata.dataflow.pipeline.steps;

import com.ikea.bigdata.exception.FailureMetaData;
import com.ikea.bigdata.protos.OrderProtos;
import com.ikea.bigdata.protos.OrderProtos.Order;
import com.ikea.bigdata.util.CommonUtil;
import com.ikea.bigdata.util.LogPipelineFailures;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataValidationFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    /*
     * Test validation transformation of valid orders
     */
    @Test
    public void testValidOrdersValidation(){
        List<OrderProtos.Order> list = getValidOrders();
        PCollectionTuple result = pipeline.apply("Create", Create.of(list))
                .apply("verification",
                        ParDo.of(new DataValidationFn()).withOutputTags(com.ikea.bigdata.common.Constants.VALID_DATA, TupleTagList.empty()));

        PAssert.that(result.get(com.ikea.bigdata.common.Constants.VALID_DATA)).containsInAnyOrder(list);
        pipeline.run();
    }

    /*
     * Test validation transformation of invalid orders
     */
    @Test
    public void testInvalidOrdersValidation(){
        List<OrderProtos.Order> list = getInvalidOrders();
        PCollectionTuple result = pipeline.apply("Create", Create.of(list))
                .apply("verification",
                        ParDo.of(new DataValidationFn())
                                .withOutputTags(com.ikea.bigdata.common.Constants.VALID_DATA,
                                        TupleTagList.of(LogPipelineFailures.FAILURE_TAG)));

        FailureMetaData failureMetadata1 = CommonUtil.getDataValidationFailureResponse(DataValidationFn.class.toString(),
                "Business Validation Failed", list.get(0).toString());
        FailureMetaData failureMetadata2 = CommonUtil.getDataValidationFailureResponse(DataValidationFn.class.toString(),
                "Business Validation Failed", list.get(1).toString());
        PCollection<FailureMetaData> dataPCollection = result.get(LogPipelineFailures.FAILURE_TAG);
        Assert.assertNotNull(dataPCollection);
        Assert.assertNotNull(dataPCollection.getPipeline());

        PAssert.that("actual",dataPCollection).containsInAnyOrder((Iterable<FailureMetaData>) Arrays.asList(failureMetadata1,failureMetadata2));
        pipeline.run();
    }

    private List<Order> getValidOrders() {
        OrderProtos.Order order1 = OrderProtos.Order.newBuilder()
                .setCost(100)
                .setModelNumber("1234")
                .setMobileNumber("2321312321")
                .setEmail("dnyaesh@gmail.com")
                .setShippingAddress("asaS SSSAS asSAsa")
                .build();
        OrderProtos.Order order2 = OrderProtos.Order.newBuilder()
                .setCost(100)
                .setModelNumber("1234")
                .setMobileNumber("2321312312")
                .setEmail("dnyaesh@gmail.com")
                .setShippingAddress("asaS SSSAS asSAsa")
                .build();

        List<OrderProtos.Order> list = new ArrayList<>();
        list.add(order1);
        list.add(order2);
        return list;
    }


    private List<OrderProtos.Order> getInvalidOrders() {
        OrderProtos.Order order1 = OrderProtos.Order.newBuilder()
                .setCost(100)
                .setModelNumber("1234")
                .setMobileNumber("23212321")
                .setEmail("dnyaesh@gmail.com")
                .setShippingAddress("asaS SSSAS asSAsa")
                .build();
        OrderProtos.Order order2 = OrderProtos.Order.newBuilder()
                .setCost(100)
                .setModelNumber("1234")
                .setMobileNumber("21312312")
                .setEmail("dnyaesh@gmail.com")
                .setShippingAddress("asaS SSSAS asSAsa")
                .build();

        List<OrderProtos.Order> list = new ArrayList<>();
        list.add(order1);
        list.add(order2);
        return list;
    }
}
