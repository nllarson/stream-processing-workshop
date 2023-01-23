package org.improving.workshop.flink;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

@Slf4j
public class TopCustomerArtistJob {

    public static final String INPUT_TOPIC = "data-demo-streams";
    public static final String KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092";
    public static final String OUTPUT_TOPIC = "kafka-workshop-flink-top-customer-artist";
    
    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ObjectNode> source = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("flink-stream-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(false)))
                .build();

        DataStream<ObjectNode> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStreamSink<String> customerStream = stream
                .keyBy(record -> record.get("value").get("customerid").asText())
                .process(new CustomerArtistCalculator())
                .print();

//        KafkaSink<TOPCUSTOMERARTI> sink = KafkaSink.<TopCustomerArtists>builder()
//            .setBootstrapServers(KAFKA_BROKERS)
//            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                .setTopic(OUTPUT_TOPIC)
//                .setValueSerializationSchema(new JsonSerde<>(TopCustomerArtists.class))
//                .build()
//            )
//            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//            .build();
//
//
//        DataStream<TopCustomerArtists> stream = env
//        .keyBy(Stream::customerid)
//        .process(new CustomerArtistCalcuator())
//        .name("top-customer-artists");
//
//        topCustomerArtistStream
//        .addSink(sink);

        env.execute("TopCustomerArtistJob");
    }

}
