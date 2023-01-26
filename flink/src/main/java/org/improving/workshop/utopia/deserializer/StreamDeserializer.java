package org.improving.workshop.utopia.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.improving.workshop.utopia.Stream;

public class StreamDeserializer implements KafkaDeserializationSchema<Stream> {
        private static final long serialVersionUID = 6099417086109694042L;

        @Override
        public Stream deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                return new ObjectMapper().readValue(record.value(), Stream.class);
        }

        @Override
        public TypeInformation<Stream> getProducedType() {
                return TypeInformation.of(Stream.class);
        }

        @Override
        public boolean isEndOfStream(Stream nextElement) {
                return false;
        }
}
