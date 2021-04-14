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
 * 窗口3秒，延迟4秒
 * 执行结果：
 * 允许延迟的时间 < 3 + 4
 * 1. 每跨越一个窗口，都会触发上一个窗口执行
 * 2. 来一个允许范围内的延迟数据会触发对应窗口执行
 * 3. 超越延迟时间(4秒)的数据将会被丢弃
 * 输入：      输出：
 * A,1        未到达时间
 * A,7        (A,1)
 * A,2        舍弃
 * A,3        (A,3)
 * A,3        (A,3)(A,3)
 * A,4        (A,3)(A,3)(A,4)
 * @author zhangbaoming
 * @date 2020/8/14 6:05 下午
 */
public class EventTimeAllowedLatenessExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = DataUtils.createDataStream(text)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(0)) {

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element) {
                        return element.f1 * 1000;
                    }
                })
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(3)))
            .allowedLateness(Time.seconds(4));
        DataUtils.process(windowedStream).print();
        env.execute("EventTime AllowedLateness");
    }
}
