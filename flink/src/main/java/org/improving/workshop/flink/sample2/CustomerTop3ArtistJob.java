package org.improving.workshop.flink.sample2;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.improving.workshop.common.ItemCount;
import org.improving.workshop.utopia.Stream;
import org.improving.workshop.utopia.deserializer.StreamDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class CustomerTop3ArtistJob {

    public static final String INPUT_TOPIC = "data-demo-streams";
    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String OUTPUT_TOPIC = "kafka-workshop-flink-customer-top3-artist";
    
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "15000"); // e.g., 2 hours

        KafkaSource<Stream> source = KafkaSource.<Stream>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("flink-stream-group-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new StreamDeserializer()))
                .build();

        KafkaSink<List<ItemCount>> sink = KafkaSink.<List<ItemCount>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer((KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new JsonSerializationSchema<List<ItemCount>>())
                        .build()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        DataStreamSink<List<ItemCount>> outputStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Music Stream Source")
                .keyBy(Stream::getCustomerid)
                .process(new CustomerArtistCounter())
                .sinkTo(sink);

        env.execute("Customer Top 3 Artist ");
    }

}
