package org.improving.workshop.flink.sample3;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.improving.workshop.utopia.Event;
import org.improving.workshop.utopia.EventTicket;
import org.improving.workshop.utopia.EventTicketConfirmation;
import org.improving.workshop.utopia.Ticket;

public class EventTicketConfirmationHandler extends CoProcessFunction<Ticket, Event, EventTicketConfirmation> {



        @Override
        public void processElement1(Ticket ticket, Context context, Collector<EventTicketConfirmation> collector) throws Exception {
Event event = eventValueState.value();
            if (event != null) {
                collector.collect(new EventTicketConfirmation(event, ticket));
            } else {
                log.warn("No event found for eventId {}", ticket.getEventid());
            }
        }


    @Override
    public void processElement2(Event value, CoProcessFunction<Ticket, Event, EventTicketConfirmation>.Context ctx, Collector<EventTicketConfirmation> out) throws Exception {
        eventValueState.update(value);
    }
}
