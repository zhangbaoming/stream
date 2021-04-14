package com.black8.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * @author zhangbaoming
 * @date 2020/9/24 10:05 上午
 */

public class TimeWindowExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of("orgId", Long.parseLong(value));
            }
        })
            .keyBy(t -> t.f0)
            .timeWindow(Time.seconds(10))
            .process(new TestTimeWindow());
        env.execute("TimeWindow");
    }

    private static class TestTimeWindow extends
        ProcessWindowFunction<Tuple2<String, Long>, Object, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements,
            Collector<Object> out)
            throws Exception {
            elements.iterator().forEachRemaining(System.out::println);
        }
    }
}
