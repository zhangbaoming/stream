package com.black8.streaming;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * An example of session windowing that keys events by ID and groups and counts them in session with gaps of 3
 * milliseconds.
 */
@Slf4j
public class SessionWindowing {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        final boolean fileOutput = params.has("output");

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 10L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 11L, 1));

        DataStream<Tuple3<String, Long, Integer>> source = env
            .addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                    for (Tuple3<String, Long, Integer> value : input) {
                        long waterMark = value.f1 - 1;
                        log.info("waterMark = {}", waterMark);
                        ctx.collectWithTimestamp(value, value.f1);
                        ctx.emitWatermark(new Watermark(waterMark));
                        Thread.sleep(100);
                    }
                    ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                }

                @Override
                public void cancel() {
                }
            });

        // We create sessions for each id with max timeout of 3 time units
        DataStream<Tuple3<String, Long, Integer>> aggregated = source
            .keyBy(0)
            .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
            .apply(new WindowFunctionTest());
//            .sum(2);

        if (fileOutput) {
            aggregated.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            aggregated.print();
        }

        env.execute();
    }

    private static class WindowFunctionTest implements
        WindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input,
            Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            log.info("start = {}, end = {}, maxTimestamp = {}", window.getStart(), window.getEnd(),
                window.maxTimestamp());
            log.info("input = {}", input);
            int size = 0;
            long time = 0;
            for (Tuple3<String, Long, Integer> tuple3 : input) {
                size++;
                time = tuple3.f1;
            }

            out.collect(new Tuple3<>(tuple.getField(0), time, size));
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements
        AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {

        long maxOutOfOrderness = 3500L;

        long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
            long timestamp = element.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }
}
