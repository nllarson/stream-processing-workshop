package org.improving.workshop.flink.sample3;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.improving.workshop.utopia.EventStatus;
import org.improving.workshop.utopia.EventTicketConfirmation;
import org.improving.workshop.utopia.Ticket;
import org.improving.workshop.utopia.TicketConfirmationStatus;

import java.util.UUID;

@Slf4j
public class TicketPurchaseHandler extends CoProcessFunction<Ticket, EventStatus, EventTicketConfirmation> {
    private transient ValueState<EventStatus> eventValueState;
    private static final OutputTag<String> ticketStatusMessages = new OutputTag<String>("ticket-status") {};
    @Override
    public void open(Configuration parameters) throws Exception {
        eventValueState = getRuntimeContext().getState(new ValueStateDescriptor<EventStatus>("eventValueState", EventStatus.class));
    }

    @Override
        public void processElement1(Ticket ticket, Context context, Collector<EventTicketConfirmation> collector) throws Exception {
            EventStatus eventStatus = eventValueState.value();
            if (eventStatus != null) {
                // process ticket sale
                eventStatus.decrementRemainingTickets(ticket);

                EventTicketConfirmation ticketConfirmation = EventTicketConfirmation.builder()
                        .confirmationid(UUID.randomUUID().toString())
                        .confirmationStatus(eventStatus.hasRemainingTickets() ? TicketConfirmationStatus.CONFIRMED : TicketConfirmationStatus.SOLD_OUT)
                        .eventStatus(eventStatus)
                        .ticketRequest(ticket)
                        .remainingTickets(eventStatus.getRemaining())
                        .build();

                collector.collect(ticketConfirmation);
                context.output(ticketStatusMessages, ticketConfirmation.statusMessage());
            } else {
                log.warn("No Event found for eventId {}", ticket.getEventid());
            }
        }

    @Override
    public void processElement2(EventStatus value, CoProcessFunction<Ticket, EventStatus, EventTicketConfirmation>.Context ctx, Collector<EventTicketConfirmation> out) throws Exception {
        eventValueState.update(value);
    }
}
