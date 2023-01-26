package org.improving.workshop.utopia.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.improving.workshop.utopia.Ticket;

public class TicketDeserializer implements KafkaDeserializationSchema<Ticket> {


    @Override
    public boolean isEndOfStream(Ticket nextElement) {
        return false;
    }

    @Override
    public Ticket deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new ObjectMapper().readValue(record.value(), Ticket.class);
    }

    @Override
        public TypeInformation<Ticket> getProducedType() {
            return TypeInformation.of(Ticket.class);
        }
}
