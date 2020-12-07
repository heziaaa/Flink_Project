package com.atguigu.app;

import com.atguigu.beans.PageViewCount;
import com.atguigu.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

public class PvCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\NetworkFlow\\src\\main\\resources\\UserBehavior.csv");

        //2、转换数据为POJO
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new UserBehavior(new Long(fileds[0]), new Long(fileds[1]), new Integer(fileds[2]), fileds[3], new Long(fileds[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        //3、对数据进行聚合统计
//        dataStream.filter(data -> "pv".equals(data.getBehavior()))
//                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2 map(UserBehavior userBehavior) throws Exception {
//                        return new Tuple2("pv", 1L);
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.hours(1))
//                .sum(1)
//                .print("agg");


        SingleOutputStreamOperator<PageViewCount> pvWindowCount = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        //创建一个随机数
                        Random random = new Random();
                        return new Tuple2(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new AggPvCount(), new WindowPvCount());
        //最终结果
        pvWindowCount.keyBy(PageViewCount::getWindowEnd)
                .process(new PvCountResult()).print();


        env.execute("page view job");
    }


    private static class AggPvCount implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long aLong) {
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

    private static class WindowPvCount implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount("url", timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    private static class PvCountResult extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
        //定义状态，保存最终结果
        ValueState<Long> resultCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            resultCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("resultCount", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<PageViewCount> collector) throws Exception {
            //最终聚合结果,resultCountState可能为null，所以在前面需要给一个默认值
            resultCountState.update(pageViewCount.getCount() + resultCountState.value());
            //创建一个定时器
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            //执行定时器，说明数据都已经来到
            out.collect(new PageViewCount("url", ctx.getCurrentKey(), resultCountState.value()));
            //清空状态
            resultCountState.clear();
        }
    }
}
