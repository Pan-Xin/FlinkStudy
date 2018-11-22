package myflink;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// here is the Test class for testing Count-Min Sketch
public class Test {

    public static final String ACC_NAME = "cmhh";
    public static CMHeavyHitter cmhhRes;

    public static void main(String[] args) throws Exception {
        cmhhRes = initCMHH();

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String dataSetPath = "C:\\Users\\xin-p\\Desktop\\workspace\\FlinkStudy\\CountMinSketch\\CountMinSketch\\src\\main\\resources\\dataset1.txt";
        String outputPath = "C:\\Users\\xin-p\\Desktop\\output.txt";

        DataStreamSource<String> dataStreamSource = env.readTextFile(dataSetPath);

        // preprocess the text file
        DataStream<Tuple2<String, Integer>> dataStream = dataStreamSource
                .flatMap(new SplitToWords())
                .flatMap(new CMHHProcess());

        dataStream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        System.out.print(cmhhRes);
    }

    private static CMHeavyHitter initCMHH() {
        double pFraction = CMHeavyHitterConfig.fraction;
        double pError = CMHeavyHitterConfig.error;
        double pConfidence = CMHeavyHitterConfig.confidence;
        int seed = CMHeavyHitterConfig.seed;
        return new CMHeavyHitter(pFraction, pError, pConfidence, seed);
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

    public static class CMHHProcess extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        public Accumulator<Object, CMHeavyHitter> globalAccumulator;
        public Accumulator<Object, CMHeavyHitter> localAccumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            globalAccumulator = getRuntimeContext().getAccumulator(ACC_NAME);
            if(globalAccumulator == null){
                getRuntimeContext().addAccumulator(ACC_NAME, new CMHeavyHitterAcc());
                globalAccumulator = getRuntimeContext().getAccumulator(ACC_NAME);
            }
            int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            localAccumulator = getRuntimeContext().getAccumulator(ACC_NAME + "-" + subTaskIndex);
            if(localAccumulator == null){
                getRuntimeContext().addAccumulator(ACC_NAME + "-" + subTaskIndex,
                        new CMHeavyHitterAcc());
                localAccumulator = getRuntimeContext().getAccumulator(ACC_NAME + "-" + subTaskIndex);
            }
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            try{
                localAccumulator.add(value);
                out.collect(new Tuple2<>(value, 1));
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            globalAccumulator.merge(localAccumulator);
            cmhhRes.merge(globalAccumulator.getLocalValue());
        }
    }
}
