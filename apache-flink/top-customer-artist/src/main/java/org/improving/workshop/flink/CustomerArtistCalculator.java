package org.improving.workshop.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CustomerArtistCalculator extends KeyedProcessFunction<String, ObjectNode, String> {

    @Override
    public void processElement(ObjectNode value, KeyedProcessFunction<String, ObjectNode, String>.Context ctx, Collector<String> out) throws Exception {
        out.collect("Streaming - " + value.get("value").get("id").asText());
    }
}
