package org.improving.workshop.flink.sample2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.improving.workshop.common.ItemCount;
import org.improving.workshop.utopia.Stream;

import java.util.ArrayList;
import java.util.List;

public class CustomerArtistCounter extends KeyedProcessFunction<String, Stream, List<ItemCount>> {

    private transient ListState<ItemCount> artistCounts;


    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ItemCount> descriptor = new ListStateDescriptor<>("artist-counts", ItemCount.class);
        artistCounts = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Stream value, KeyedProcessFunction<String, Stream, List<ItemCount>>.Context ctx, Collector<List<ItemCount>> out) throws Exception {
        List<ItemCount> allCounts = new ArrayList<>();
        boolean found = false;

        for(ItemCount count : artistCounts.get()) {
            allCounts.add(count);
        }

        for (ItemCount count : allCounts) {
            if (count.getId().equals(value.getArtistid())) {
                count.setCount(count.getCount() + 1);
                artistCounts.update(allCounts);
                found = true;
            }
        }

        if (!found) {
            allCounts.add(new ItemCount(value.getArtistid(), 1L));
            artistCounts.update(allCounts);
        }

        List<ItemCount> top3 = new ArrayList<>();
        allCounts.stream().sorted((a, b) -> b.getCount().compareTo(a.getCount())).limit(3).forEach(top3::add);

        System.out.println("Customer " + value.getCustomerid() + " top artists: " + top3);
        out.collect(top3);
    }
}
