package myflink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItems{
    public static void main(String args[]) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置全局的并发为1
        env.setParallelism(1);

        // 创建一个POJO CSV INPUT FORMAT,读取CSV并转为指定的POJO类型
        // UserBehavior的本地路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取UserBehavior的TypeInformation，是一个PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于Java反射抽取出的字段顺序是不确定的，因此需要显示指定文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "cateId", "behavior", "timestamp"};
        // 创建PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        // 创建输入源
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);

        // 设置Flink按照EventTime模式处理,默认按照ProcessingTime进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 获得业务时间以及生成waterMark
        // 本数据源是已经整理过，时间戳是递增的，因此使用AscendingTimestampExtractor进行时间戳的抽取和waterMark的生成
        // 对于乱序一般使用BoundedOutOfOrdernessTimestampExtrator
        DataStream<UserBehavior> timeData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.timestamp * 1000; // 将原始数据的毫秒转为秒
                    }
                });

        // 需求：每隔五分钟输出过去一小时点击量最多的前N个商品
        // 将点击行为的数据过滤出来
        DataStream<UserBehavior> pvData = timeData
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return value.behavior.equals("pv");
                    }
                });

        // 窗口大小应该为1小时（统计的范围），窗口滑动的时间为5min（sliding window）
        // 得到每个商品在每个窗口点击量的数据流
        DataStream<ItemViewCount> windowedData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        // 计算TopN最热门商品
        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));

        // 打印输出并提交任务
        topItems.print();
        env.execute("Hot Item Job");

    }

    // KeyedProcessFunction K I O
    // key 为窗口时间戳
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize){
            this.topSize = topSize;
        }

        // 用户存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发TopN计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration config) throws Exception{
            super.open(config);
            ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class
            );
            itemState = getRuntimeContext().getListState(itemStateDesc);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value); // 每条数据都保存到状态中
            // 注册windowEnd+1的eventTime timer，当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for(ItemViewCount item: itemState.get()){
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int)(o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化为String
            StringBuilder res = new StringBuilder();
            res.append("=====================\n");
            res.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for(int i=0; i<topSize; i++){
                ItemViewCount curItem = allItems.get(i);
                res.append("No. ").append(i).append(":")
                        .append(" 商品ID=").append(curItem.itemId)
                        .append(" 浏览量=").append(curItem.viewCount)
                        .append("\n");
            }
            res.append("====================\n\n");
            out.collect(res.toString());
        }

    }

    // AggregateFunction的三个参数IN ACC OUT
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // WindowFunction IN OUT K W
    // 用户输出窗口的结果
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(
                Tuple key, // 窗口主键，itemId
                TimeWindow window, // 窗口
                Iterable<Long> aggRes, // 聚合函数结果
                Collector<ItemViewCount> out // 输出
                ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggRes.iterator().next();
            out.collect(ItemViewCount.of(itemId, window.getEnd(), count));

        }
    }
}