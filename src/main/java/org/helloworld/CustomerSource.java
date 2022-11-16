package org.helloworld;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomerSource {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> customerSource = environment.addSource(new StringSourceFunction(), "customerSource");
        customerSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                collector.collect(Tuple2.of(s, 1L));
            }
        }).keyBy((KeySelector<Tuple2<String, Long>, String>) stringLongTuple2 -> stringLongTuple2.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                /*.aggregate(new AggregateFunction<Tuple2<String, Long>, Map<String, Integer>, Map<String, Integer>>() {
                    @Override
                    public Map<String, Integer> createAccumulator() {
                        return new HashMap();
                    }

                    @Override
                    public Map<String, Integer> add(Tuple2<String, Long> stringLongTuple2, Map<String, Integer> map) {
                        Integer value = map.getOrDefault(stringLongTuple2.f0, 0)+1;
                        map.put(stringLongTuple2.f0, value);
                        return map;
                    }

                    @Override
                    public Map<String, Integer> getResult(Map<String, Integer> stringIntegerMap) {
                        return stringIntegerMap;
                    }

                    @Override
                    public Map<String, Integer> merge(Map<String, Integer> stringIntegerMap, Map<String, Integer> acc1) {
                        return null;
                    }
                }).print();*/
                .sum(1).print();
        environment.execute("timeWindow");
    }

    private static class StringSourceFunction implements SourceFunction<String> {
        volatile boolean flag = false;
        String[] array = new String[]{"java", "python", "golang", "c++", "scala", "kotlin", "erlang"};
        AtomicInteger index = new AtomicInteger(0);

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (!flag) {
                long timestamps = System.currentTimeMillis();
                Watermark watermark = new Watermark(timestamps);
                ctx.collectWithTimestamp(array[index.getAndIncrement()%array.length], timestamps);
                ctx.emitWatermark(watermark);
                Thread.sleep(index.get()* 10L);
            }
        }

        @Override
        public void cancel() {
            flag = true;
        }
    }
}
