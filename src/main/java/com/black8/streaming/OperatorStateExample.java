package com.black8.streaming;

import com.black8.streaming.util.DataUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态 (Operator State)：顾名思义，状态是和算子进行绑定的，一个算子的状态不能被其他算子所访问到。
 * 官方文档上对 Operator State 的解释是：each operator state is bound to one parallel operator instance，
 * 所以更为确切的说一个算子状态是与一个并发的算子实例所绑定的，即假设算子的并行度是 2，那么其应有两个对应的算子状态
 *
 * @author zhangbaoming
 * @date 2020/8/13 7:14 下午
 */
public class OperatorStateExample {

    public static void main(String[] args) throws Exception {
        int port = 7789;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        DataUtils.createDataStream(text).map(new ThresholdWarningFunction()).print();
        env.execute("StateExample");
    }
}
