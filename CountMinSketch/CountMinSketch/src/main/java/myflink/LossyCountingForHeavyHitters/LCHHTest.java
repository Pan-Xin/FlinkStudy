package myflink.LossyCountingForHeavyHitters;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class LCHHTest {
    public static final String ACC_NAME = "lchh";
    public static LossyCounting lchhRes;
    public static double pFraction = LCHeavyHitterConfig.pFraction;
    public static double pError = LCHeavyHitterConfig.pError;

    public static void main(String[] args) throws Exception {
        lchhRes = initLCHH();

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String dataSetPath = "C:\\Users\\xin-p\\Desktop\\workspace\\FlinkStudy\\CountMinSketch\\CountMinSketch\\src\\main\\resources\\dataset1.txt";
        String outputPath = "C:\\Users\\xin-p\\Desktop\\dataset1Words.txt";

        DataStreamSource<String> dataStreamSource = env.readTextFile(dataSetPath);

        // preprocess the text file
        DataStream<String> dataStream = dataStreamSource
                .flatMap(new SplitToWords())
                .flatMap(new LCHHProcess());


        dataStream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        System.out.print(lchhRes);
    }

    private static LossyCounting initLCHH() {
        return new LossyCounting(pFraction, pError);
    }


    // used to split the sentences into words
    public static class SplitToWords implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] str1 = value.split("\n");
            for(String str : str1){
                str = str.replaceAll("\\p{P}"," "); // remove punctuation
                String[] str2 = str.split(" ");
                for(String word : str2)
                    if(!"".equals(word))
                        out.collect(word);
            }
        }
    }

    public static class LCHHProcess extends RichFlatMapFunction<String, String> {
        public Accumulator<Object, LossyCounting> globalAccumulator;
        public Accumulator<Object, LossyCounting> localAccumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            globalAccumulator = getRuntimeContext().getAccumulator(ACC_NAME);
            if(globalAccumulator == null){
                getRuntimeContext().addAccumulator(ACC_NAME, new LCHeavyHitterAcc());
                globalAccumulator = getRuntimeContext().getAccumulator(ACC_NAME);
            }
            int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            localAccumulator = getRuntimeContext().getAccumulator(ACC_NAME + "-" + subTaskIndex);
            if(localAccumulator == null){
                getRuntimeContext().addAccumulator(ACC_NAME + "-" + subTaskIndex,
                        new LCHeavyHitterAcc());
                localAccumulator = getRuntimeContext().getAccumulator(ACC_NAME + "-" + subTaskIndex);
            }
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            try{
                localAccumulator.add(value);
                out.collect(value + "," + 1);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            globalAccumulator.merge(localAccumulator);
            lchhRes.merge(globalAccumulator.getLocalValue());
        }
    }
}
