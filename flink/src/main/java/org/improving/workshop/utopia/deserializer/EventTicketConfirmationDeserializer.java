package org.improving.workshop.utopia.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.improving.workshop.utopia.EventTicketConfirmation;

public class EventTicketConfirmationDeserializer  implements KafkaDeserializationSchema<EventTicketConfirmation> {

    @Override
    public EventTicketConfirmation deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new ObjectMapper().readValue(record.value(), EventTicketConfirmation.class);
    }

    @Override
    public TypeInformation<EventTicketConfirmation> getProducedType() {
        return TypeInformation.of(EventTicketConfirmation.class);
    }

    @Override
    public boolean isEndOfStream(EventTicketConfirmation nextElement) {
        return false;
    }
}
