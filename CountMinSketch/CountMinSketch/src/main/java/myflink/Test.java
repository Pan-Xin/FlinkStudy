package myflink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple1;

import java.io.Serializable;
import java.util.Random;

// here is the Test class for testing Count-Min Sketch
public class Test {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        String dataSetPath = "C:\\Users\\xin-p\\Desktop\\workspace\\FlinkStudy\\CountMinSketch\\CountMinSketch\\src\\main\\resources\\dataset1.txt";
        String outputPath = "C:\\Users\\xin-p\\Desktop\\output.txt";

        DataStreamSource<String> dataStreamSource = env.readTextFile(dataSetPath);

        // preprocess the text file
        DataStream<CMHeavyHitter> dataStream = dataStreamSource
                .flatMap(new SplitToWords())
                .flatMap(new CMHHProcess());

        dataStream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult res = env.execute();
        res.getJobExecutionResult();

    }

    // used to split the sentences into words
    public static class SplitToWords implements FlatMapFunction<String, String>{

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] str1 = value.split("\n");
            for(String str : str1){
                str = str.replaceAll("\\p{P}"," "); // remove punctuation
                String[] str2 = str.split(" ");
                for(String word : str2)
                    out.collect(word);
            }
        }
    }

    // the configuration for count-min sketch heavy hitter
    public static class CMHHConfig{
        static final double fraction = 0.01;
        static final double error = 0.005;
        static final double confidence = 0.99;
        static final int seed = 7362181;
        static final Random random = new Random();
        static final int cardinality = 1000000;
        static final int maxScale = 1000000;
    }

    public static class CMHHProcess extends RichFlatMapFunction<String, Tuple1<Integer>> {

        private Accumulator<Object, Serializable> globalAcc;
        private Accumulator<Object, Serializable> localAcc;

        private static final String ACC_NAME = "CMHH";

        @Override
        public void open(Configuration parameters) throws Exception {
            globalAcc = getRuntimeContext().getAccumulator("CMHH");
            if(globalAcc == null){
                getRuntimeContext().addAccumulator(ACC_NAME,
                        new CMHeavyHitter(CMHHConfig.fraction, CMHHConfig.error, CMHHConfig.confidence, CMHHConfig.seed));
            }
        }

        @Override
        public void flatMap(String value, Collector<Tuple1<Integer>> out) throws Exception {

        }
    }
}
