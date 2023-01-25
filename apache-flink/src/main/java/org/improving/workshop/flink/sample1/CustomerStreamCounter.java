package org.improving.workshop.flink.sample1;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.improving.workshop.utopia.pojo.Stream;

@Slf4j
public class CustomerStreamCounter extends KeyedProcessFunction<String, Stream, Long> {

    private transient ValueState<Long> streamCounter;

    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("streamCounter", Types.LONG);
        streamCounter = getRuntimeContext().getState(descriptor);
    }
    @Override
    public void processElement(Stream value, KeyedProcessFunction<String, Stream, Long>.Context ctx, Collector<Long> out) throws Exception {
        Long currentCount = streamCounter.value();

        if (currentCount == null) {
            currentCount =0L;
        }

        currentCount++;
        streamCounter.update(currentCount);

        System.out.println("Customer " + value.getCustomerid() + " has streamed " + currentCount + " times.");

        out.collect(currentCount);
    }
}
