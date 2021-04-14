package com.black8.streaming;

import com.black8.streaming.util.DataUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zhangbaoming
 * @date 2020/2/12 3:49 下午
 */
public class WaterMarkExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = DataUtils.createDataStream(text)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(5)) {

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element) {
                        return element.f1 * 1000;
                    }
                })
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(3)));
        DataUtils.process(windowedStream).print();
        env.execute("WaterMark AllowedLateness");
    }
}
