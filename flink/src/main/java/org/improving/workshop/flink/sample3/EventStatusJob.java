package org.improving.workshop.flink.sample3;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.improving.workshop.utopia.Event;
import org.improving.workshop.utopia.EventTicketConfirmation;
import org.improving.workshop.utopia.Ticket;
import org.improving.workshop.utopia.deserializer.EventDeserializer;
import org.improving.workshop.utopia.deserializer.EventTicketConfirmationDeserializer;
import org.improving.workshop.utopia.deserializer.TicketDeserializer;

import java.util.Properties;
import java.util.UUID;

public class EventStatusJob {

    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String TICKET_TOPIC = "data-demo-tickets";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "150000"); // e.g., 2 hours

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        // create kafkasource for tickets topic
        KafkaSource<Ticket> ticketKafkaSource = KafkaSource.<Ticket>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TICKET_TOPIC)
                .setGroupId("flink-stream-ticket-purchase-tickets-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(KafkaRecordDeserializationSchema.of(new TicketDeserializer()))
                .build();

        DataStream<Ticket> ticketStream = env.fromSource(ticketKafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource-Tickets")
                .name("kafkaTicketSource")
                .uid("kafkaTicketSource");

        tableEnv.createTemporaryView("tickets", ticketStream);

        tableEnv.executeSql("SELECT eventid as EventId, count(*) as TicketsSold FROM tickets GROUP BY eventid").print();
    }
}
