package com.black8.streaming;

import com.black8.streaming.util.DataUtils;
import java.util.List;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 键控状态 (Keyed State) ：是一种特殊的算子状态，即状态是根据 key 值进行区分的，Flink 会为每类键值维护一个状态实例。
 *
 * @author zhangbaoming
 * @date 2020/8/13 3:42 下午
 */
public class KeyedStateExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        DataUtils.createDataStream(text).keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Object>() {

            private transient ListState<Long> abnormalData;

            @Override
            public void open(Configuration parameters) throws Exception {
                abnormalData = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("abnormalData", Long.class));
            }

            @Override
            public Object map(Tuple2<String, Long> value) throws Exception {
                if (value.f1 == 0) {
                    return 0;
                }
                if (value.f1 > 90) {
                    abnormalData.add(value.f1);
                }
                List<Long> abnormalDataList = Lists.newArrayList(abnormalData.get().iterator());
                if (abnormalDataList.size() > 2) {
                    String hint = String.format("key:%s异常超过阈值:%s", value.f0, abnormalDataList.toString());
                    abnormalData.clear();
                    return hint;
                }
                return value;
            }
        }).print();
        env.execute("StateExample");
    }
}
