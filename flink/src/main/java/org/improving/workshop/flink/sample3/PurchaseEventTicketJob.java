package org.improving.workshop.flink.sample3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.improving.workshop.utopia.*;
import org.improving.workshop.utopia.deserializer.EventDeserializer;
import org.improving.workshop.utopia.deserializer.TicketDeserializer;

import java.util.UUID;

public class PurchaseEventTicketJob {
    // TODO: externalize input topics to other class
    public static final String EVENT_TOPIC = "data-demo-events";
    public static final String TICKET_TOPIC = "data-demo-tickets";
    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String OUTPUT_TOPIC = "kafka-workshop-flink-purchase-event-ticket";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create kafkasource for events topic
        KafkaSource<Event> eventKafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(EVENT_TOPIC)
                .setGroupId("flink-stream-ticket-purchase-events-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new EventDeserializer()))
                .build();

        // create kafkasource for tickets topic
        KafkaSource<Ticket> ticketKafkaSource = KafkaSource.<Ticket>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TICKET_TOPIC)
                .setGroupId("flink-stream-ticket-purchase-tickets-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new TicketDeserializer()))
                .build();




        DataStream<Event> eventStream = env.fromSource(eventKafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource-Events")
                .name("kafkaEventSource")
                .uid("kafkaEventSource");

        DataStream<Ticket> ticketStream = env.fromSource(ticketKafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "KafkaSource-Tickets")
                .name("kafkaTicketSource")
                .uid("kafkaTicketSource");

        DataStream<EventTicketConfirmation> eventTicketConfirmationStream = ticketStream
                .keyBy(Ticket::getEventid)
                .connect(eventStream.keyBy(Event::getId))
                .process(new EventTicketHandler())
                .process(new EventTicketConfirmationHandler())
                .name("eventTicketConfirmationProcessFunction")
                .uid("eventTicketConfirmationProcessFunction");

        // create datastream to join tickets and events on eventid output event status


            // sold out
            // confirmed -- limited availability
            // confirmed
        // create kafkasink for ticket-confirmation topic


    }

}
