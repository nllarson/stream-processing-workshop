package org.improving.workshop.utopia.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.improving.workshop.utopia.Event;

public class EventDeserializer implements KafkaDeserializationSchema<Event> {

    @Override
    public Event deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new ObjectMapper().readValue(record.value(), Event.class);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

}
