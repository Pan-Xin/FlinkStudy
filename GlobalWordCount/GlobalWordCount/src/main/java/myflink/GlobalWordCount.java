package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

// 之前的wordCount只是计算单个窗口内的频率，这个是全局的频率
public class GlobalWordCount {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 连接localHost port=12345 输入流
		DataStream<String> text = env.socketTextStream("localhost", 12345, "\n");

		DataStream<Tuple2<String, Integer>> globalSum = text
				.flatMap(new WordSplitter())
				.keyBy(0)
				.process(new GlobalSum());
		// 如果用.sum的话，将会在每一个数据到达的时候就计算一次


		globalSum.print();
		env.execute("Global Word Count");
	}


	// 一行可能出现多个单词，按照空格进行划分
	public static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String[] words = value.split(" ");
			for(String word: words){
				out.collect(Tuple2.of(word, 1));
			}
		}
	}


	public static class GlobalSum extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private ListState<Tuple2<String, Integer>> listState; // 存储中间值

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<Tuple2<String, Integer>> listStateDesc = new ListStateDescriptor<Tuple2<String, Integer>>(
					"listState-state",
					TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
			);
			listState = getRuntimeContext().getListState(listStateDesc);
		}

		@Override
		public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
			listState.add(value);
			ctx.timerService().registerProcessingTimeTimer(0);// 当processing time超过这个的时候就触发onTimer
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			HashMap<String, Integer> map = new HashMap<>();
			// 统计出现的频率
			for(Tuple2<String, Integer> temp: listState.get()){
				String word = temp.f0;
				Integer cont = temp.f1;
				if(map.containsKey(word))
					cont += map.get(word);
				map.put(word, cont);
			}
			for(String word: map.keySet()){
				out.collect(Tuple2.of(word, map.get(word)));
			}
		}
	}
}
