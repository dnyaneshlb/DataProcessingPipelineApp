package com.ikea.bigdata.common;

public class Constants {

    private Constants(){

    }

    public static final String INSERT_ORDER_QUERY = "INSERT INTO public.\"OrderDetails\"(\n" +
            "\tid, cost, email, shipping_address)\n" +
            "\tVALUES (?, ?, ?, ?)";

    public static final String POSTGRESS_DRIVER_CLASS = "org.postgresql.Driver";

    public static final String CSV_HEADER_ID = "Product ID";
    public static final String CSV_HEADER_EMAIL = "Email";
    public static final String CSV_HEADER_SHIPPING_ADDRESS = "Shipping Address";
    public static final String CSV_HEADER_COST = "Cost";

    public static final String NAME_TEMPLATE = "template";
    public static final String FILE_NAME_SUFFIX = "orders";
    public static final String FILE_TYPE_SUFFIX = ".csv";
    public static final Integer NUM_OF_SHARDS = 1;

    public static final String SEPERATOR_COMMA = ",";

}
