package org.improving.workshop.flink.sample1;

import java.util.Properties;
import java.util.UUID;

public class Workshop1 {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String OUTPUT_TOPIC = "kafka-workshop-flink-workshop-1-output";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "15000"); // e.g., 2 hours

        env.execute("Workshop 1");
    }
}
