package com.black8.streaming.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * @author zhangbaoming
 * @date 2020/8/13 7:19 下午
 */
public class DataUtils {

    public static SingleOutputStreamOperator<Tuple2<String, Long>> createDataStream(DataStream<String> text) {
        return text.map(
            new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    String[] arr = value.split(",");
                    if (arr.length == 2) {
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]));
                    }
                    return Tuple2.of("default", 0L);
                }
            });
    }

    public static SingleOutputStreamOperator<String> process(
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> data) {
        return data.process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements,
                Collector<String> out) throws Exception {
                StringBuilder sbl = new StringBuilder();
                elements.forEach(sbl::append);
                out.collect(sbl.toString());
            }
        });
    }
}
