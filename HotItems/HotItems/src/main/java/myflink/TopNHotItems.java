package myflink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public interface TopNHotItems {
    void OnTimer(long timestamp, KeyedProcessFunction.OnTimerContext ctx, Collector<String> out) throws Exception;
}
