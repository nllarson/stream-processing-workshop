package org.improving.workshop.flink.sample3;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.improving.workshop.utopia.Event;
import org.improving.workshop.utopia.EventTicket;
import org.improving.workshop.utopia.Ticket;

@Slf4j
public class EventTicketHandler extends CoProcessFunction<Ticket, Event, EventTicket> {
    private transient ValueState<Event> eventValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.debug("{}, Initializing state...", this.getClass().getSimpleName());
        eventValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("eventValueState", Event.class));
    }

    @Override
    public void processElement1(Ticket ticket, Context context, Collector<EventTicket> collector) throws Exception {
        Event event = eventValueState.value();
        if (event != null) {
            collector.collect(new EventTicket(event, ticket));
        } else {
            log.warn("No event found for eventId {}", ticket.getEventid());
        }
    }

    @Override
    public void processElement2(Event value, CoProcessFunction<Ticket, Event, EventTicket>.Context ctx, Collector<EventTicket> out) throws Exception {
        eventValueState.update(value);
    }


}
