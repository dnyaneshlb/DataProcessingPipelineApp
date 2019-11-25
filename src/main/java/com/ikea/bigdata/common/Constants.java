package com.ikea.bigdata.common;

import com.ikea.bigdata.protos.OrderProtos;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.values.TupleTag;

@NoArgsConstructor(access= AccessLevel.PRIVATE)
public class Constants {

    /**
     * For output tuple tags, we should use below syntax.
     * Note it is different from what we use for side input
     */
    public static final TupleTag<OrderProtos.Order> VALID_DATA = new TupleTag<OrderProtos.Order>(){};
    public static final String ATTRIBUTE_ID = "Order";
    public static final String CSV_HEADER_MOBILE_NUMBER = "Mobile Number";

    public static final String INSERT_ORDER_QUERY = "INSERT INTO public.\"OrderDetails\"(\n" +
            "\tmodel_number, cost, email, shipping_address, mobile_number)\n" +
            "\tVALUES (?, ?, ?, ?, ?)";

    public static final String POSTGRESS_DRIVER_CLASS = "org.postgresql.Driver";

    public static final String CSV_MODEL_NUMBER = "Product ID";
    public static final String CSV_HEADER_EMAIL = "Email";
    public static final String CSV_HEADER_SHIPPING_ADDRESS = "Shipping Address";
    public static final String CSV_HEADER_COST = "Cost";

    public static final String NAME_TEMPLATE = "template";
    public static final String FILE_NAME_PREFIX = "orders";
    public static final String FILE_TYPE_SUFFIX = ".csv";
    public static final Integer NUM_OF_SHARDS = 5;

    public static final String SEPARATOR_COMMA = ",";
    
    

}
