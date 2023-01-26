package org.improving.workshop.flink.sample3;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.improving.workshop.utopia.Event;
import org.improving.workshop.utopia.EventStatus;
import org.improving.workshop.utopia.EventTicket;
import org.improving.workshop.utopia.Ticket;

@Slf4j
public class EventStatusHandler extends KeyedProcessFunction<String, Event, EventStatus> {
    private transient ValueState<EventStatus> eventStatusValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.debug("{}, Initializing state...", this.getClass().getSimpleName());
        eventStatusValueState = getRuntimeContext().getState(new ValueStateDescriptor<EventStatus>("eventValueState", EventStatus.class));
    }


    @Override
    public void processElement(Event event, KeyedProcessFunction<String, Event, EventStatus>.Context ctx, Collector<EventStatus> out) throws Exception {
        EventStatus eventStatus = eventStatusValueState.value();

        if (eventStatus == null) {
            eventStatus = new EventStatus(event);
        }

        out.collect(eventStatus);
    }
}
