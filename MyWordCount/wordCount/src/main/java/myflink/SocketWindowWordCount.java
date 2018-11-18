package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 创建一个socket流数据源，host=localhost && port=12345
		DataStream<String>  text = env.socketTextStream("localhost", 12345, "\n");

		DataStream<Tuple2<String, Integer>> windowCount = text
				.flatMap(new WordCount())
				.keyBy(0)
				.timeWindow(Time.seconds(10))
				.sum(1);

		// 使用一个线程去打印
		windowCount.print().setParallelism(1);

		// execute program
		env.execute("SocketWindowWordCount");
	}

	public static final class WordCount implements FlatMapFunction<String, Tuple2<String, Integer>>{
		@Override
		// 一行中可能存在多个字母，按空格划分
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String words[] = value.split("\\s");
			for(String word: words){
				out.collect(new Tuple2<>(word, 1));
			}
		}
	}
}
