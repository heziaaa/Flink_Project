package com.atguigu.exer;

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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;

public class ItemCount_Test {
    public static void main(String[] args) throws Exception {
        //1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2、读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\HottlemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        //3、转换为POJO
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] words = line.split(",");
            return new UserBehavior(new Long(words[0]), new Long(words[1]), new Integer(words[2]), words[3], new Long(words[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {//获取事件时间，有序数据

            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
        //4、过滤分组开窗聚合
        DataStream<ItemViewCount> aggCountStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new MyItemCount(), new MyWindow());
        //5、对聚合后的结果通过windowEnd开窗，统计count并求出topN
        DataStream<String> resultStream = aggCountStream.keyBy("windowEnd")
                .process(new MyProcess(5));

        resultStream.print();
        env.execute("hot items top5");
    }

    private static class MyItemCount implements AggregateFunction<UserBehavior, Long, Long> {
        //创建一个状态
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

    private static class MyWindow implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            long itemId = tuple.getField(0);
            long count = iterable.iterator().next();
            long windowEnd = timeWindow.getEnd();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    private static class MyProcess extends ProcessFunction<ItemViewCount, String> {
        private Integer topNSize;

        public MyProcess(int i) {
            this.topNSize = i;
        }

        //声明状态
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //为状态赋值
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("topNState", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //将数据保存到状态中
            itemViewCountListState.add(itemViewCount);
            //创建一个定时任务
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        //执行定时任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取topN,
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            //排序
            Collections.sort(itemViewCounts);
            //将排名好的数据格式化成String ，输出
            StringBuilder result = new StringBuilder();
            result.append("========================\n");
            result.append("商品结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(topNSize,itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                result.append("商品id：").append(itemViewCount.getItemId()).append(",");
                result.append("热门度：").append(itemViewCount.getCount());
                result.append("\n");
            }
            result.append("========================\n");

            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
