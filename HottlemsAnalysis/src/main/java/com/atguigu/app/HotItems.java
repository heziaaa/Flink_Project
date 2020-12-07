package com.atguigu.app;

import com.atguigu.beans.ItemViewCount;
import com.atguigu.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;


public class HotItems {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\HottlemsAnalysis\\src\\main\\resources\\UserBehavior.csv");


        //转换为javabean
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] words = line.split(",");
            return new UserBehavior(new Long(words[0]), new Long(words[1]), new Integer(words[2]), words[3], new Long(words[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        //4、分组开窗聚合
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤出pv数据)
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowCountResult());

        //5、收集同一窗口内的所有count数据，排序输出Top N
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd") //对窗口分组
                .process(new TopNHotItems(5));
        resultStream.print();

        //执行
        env.execute("hot items job");
    }

    //实现自定义方法
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }


        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class WindowCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }


    //实现自定义的 keyedProcessFunction
    private static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private Integer topSize;

        public TopNHotItems(int i) {
            this.topSize = i;
        }

        //声明状态
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCountList", ItemViewCount.class));

        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //没来一个数据保存到状态中
            itemViewCountListState.add(itemViewCount);
            //注册定时器，windowEnd+1 后触发
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发定时器，所有数据到齐，排序topN
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            //SORT排序
            Collections.sort(itemViewCounts);
            //将排名后的数据格式化成string 方便打印输出
            StringBuilder result = new StringBuilder();
            result.append("===================\n");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            //变量，输出topN
            for (int i = 0; i < Math.min(topSize,itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                result.append("NO").append(i+1).append(":")
                        .append("商品id = ").append(currentItemViewCount.getItemId())
                        .append("热门度 = " ).append(currentItemViewCount.getCount())
                        .append("\n");
            }
            result.append("===================\n");

            //控制输出频率
            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
