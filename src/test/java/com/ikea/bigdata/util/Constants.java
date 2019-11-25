package com.ikea.bigdata.util;

public class Constants {

    public static final String INGESTION_SUBSCRIPTION_KEY = "eventSubscription";
    public static final String INGESTION_SUBSCRIPTION = "projects/ikea-sandbox-258205/subscriptions/sub1";

    public static final String FAILURE_TOPIC_KEY = "failureDataTopic";
    public static final String FAILURE_TOPIC = "projects/ikea-sandbox-258205/topics/failed-messages-topic";

    public static final String DATABASE_URL_KEY = "databaseURL";
    public static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/postgres";

    public static final String DATABASE_USER_NAME_KEY = "databaseUserName";
    public static final String DATABASE_USER_NAME = "postgres";

    public static final String DATABASE_PWD_KEY = "databasePassword";
    public static final String DATABASE_PWD = "postgres";

    public static final String PROJECT_KEY = "project";
    public static final String PROJECT_ID = "ikea-sandbox";

    public static final String RUNNER_KEY = "runner";
    public static final String RUNNER = "DirectRunner";

    public static final String PATTERN = "--%s=%s";

}
