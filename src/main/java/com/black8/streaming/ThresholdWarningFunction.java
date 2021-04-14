package com.black8.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 *
 * @author zhangbaoming
 * @date 2020/8/13 7:22 下午
 */
public class ThresholdWarningFunction extends RichMapFunction<Tuple2<String, Long>, Object>
    implements CheckpointedFunction {

    private List<Long> bufferedData = new ArrayList<>();

    private transient ListState<Long> abnormalData;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        abnormalData = context.getOperatorStateStore().
            getListState(new ListStateDescriptor<>("abnormalData",
                TypeInformation.of(new TypeHint<Long>() {
                })));
        if (context.isRestored()) {
            for (Long element : abnormalData.get()) {
                bufferedData.add(element);
            }
        }
    }

    @Override
    public Object map(Tuple2<String, Long> value) throws Exception {
        if (value.f1 == 0) {
            return 0;
        }
        if (value.f1 > 80) {
            bufferedData.add(value.f1);
        }
        if (bufferedData.size() > 2) {
            String hint = String.format("key:%s异常超过阈值:%s", value.f0, bufferedData.toString());
            bufferedData.clear();
            return hint;
        }
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        abnormalData.clear();
        for (Long element : bufferedData) {
            abnormalData.add(element);
        }
    }
}
