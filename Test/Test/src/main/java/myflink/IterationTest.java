package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 测试iteration，将数一直减1直到为0
public class IterationTest {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> data = env.generateSequence(0, 1000);

		IterativeStream iteration = data.iterate();

		DataStream<Long> minusData = iteration.map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return value - 1;
			}

		});

		DataStream<Long> greaterThanOne = minusData.filter(new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				return value > 0;
			}
		});

		iteration.closeWith(greaterThanOne);

		data.print();

		env.execute("Test is working");
	}
}
