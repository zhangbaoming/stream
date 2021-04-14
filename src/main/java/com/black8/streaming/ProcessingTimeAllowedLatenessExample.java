package com.black8.streaming;

import com.black8.streaming.util.DataUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *
 * @author zhangbaoming
 * @date 2020/8/15 3:43 下午
 */
public class ProcessingTimeAllowedLatenessExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        SingleOutputStreamOperator<Tuple2<String, Long>> dataStream = DataUtils
            .createDataStream(text);

        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream =
            dataStream
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.seconds(2))
                .allowedLateness(Time.seconds(4));
        DataUtils.process(windowedStream).print();
        env.execute("ProcessingTime Example");
    }
}
