package org.improving.workshop.flink.sample3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.improving.workshop.utopia.Event;
import org.improving.workshop.utopia.EventStatus;
import org.improving.workshop.utopia.EventTicketConfirmation;
import org.improving.workshop.utopia.Ticket;
import org.improving.workshop.utopia.deserializer.EventDeserializer;
import org.improving.workshop.utopia.deserializer.TicketDeserializer;

import java.util.Properties;
import java.util.UUID;

public class PurchaseEventTicketJob {
    // TODO: externalize input topics to other class
    public static final String EVENT_TOPIC = "data-demo-events";
    public static final String TICKET_TOPIC = "data-demo-tickets";
    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String OUTPUT_TOPIC = "kafka-workshop-flink-purchase-event-ticket";
    private static final OutputTag<String> ticketStatusMessages = new OutputTag<String>("ticket-status") {};

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "150000"); // e.g., 2 hours

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create kafkasource for events topic
        KafkaSource<Event> eventKafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(EVENT_TOPIC)
                .setGroupId("flink-stream-ticket-purchase-events-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(KafkaRecordDeserializationSchema.of(new EventDeserializer()))
                .build();

        // create kafkasource for tickets topic
        KafkaSource<Ticket> ticketKafkaSource = KafkaSource.<Ticket>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TICKET_TOPIC)
                .setGroupId("flink-stream-ticket-purchase-tickets-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(KafkaRecordDeserializationSchema.of(new TicketDeserializer()))
                .build();

        KafkaSink<EventTicketConfirmation> sink = KafkaSink.<EventTicketConfirmation>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer((KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new JsonSerializationSchema<EventTicketConfirmation>())
                        .build()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        DataStream<Event> eventStream = env.fromSource(eventKafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource-Events")
                .name("kafkaEventSource")
                .uid("kafkaEventSource");

        DataStream<Ticket> ticketStream = env.fromSource(ticketKafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "KafkaSource-Tickets")
                .name("kafkaTicketSource")
                .uid("kafkaTicketSource");

        DataStream<EventStatus> eventStatusStream = eventStream
                .keyBy(Event::getId)
                .process(new EventStatusHandler())
                .name("eventStatusProcessFunction")
                .uid("eventStatusProcessFunction");

        SingleOutputStreamOperator<EventTicketConfirmation> eventTicketConfirmationStream = ticketStream
                .keyBy(Ticket::getEventid)
                .connect(eventStatusStream.keyBy(value -> value.getEvent().getId()))
                .process(new TicketPurchaseHandler())
                .name("eventTicketConfirmationProcessFunction")
                .uid("eventTicketConfirmationProcessFunction");

        DataStreamSink<EventTicketConfirmation> eventTicketSink = eventTicketConfirmationStream
                .sinkTo(sink)
                .name("eventTicketSink")
                .uid("eventTicketSink");

        DataStreamSink<String> eventTicketStatusStream = eventTicketConfirmationStream.getSideOutput(ticketStatusMessages).print();

        env.execute("Purchase Event Ticket");
    }

}
