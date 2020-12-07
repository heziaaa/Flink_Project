package com.atguigu.app;

import com.atguigu.beans.PageViewCount;
import com.atguigu.beans.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class UvCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2、读取文件
        DataStreamSource<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\NetworkFlow\\src\\main\\resources\\UserBehavior.csv");
        //3、转换成POJO
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fileds = line.split(",");
            return new UserBehavior(new Long(fileds[0]), new Long(fileds[1]), new Integer(fileds[2]), fileds[3], new Long(fileds[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
        //4、过滤，分组，开窗，聚合
        SingleOutputStreamOperator<PageViewCount> resultStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        resultStream.print("uv count");
        env.execute();
    }

    private static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            //定义一个Set结构，将iterable 添加到Set中,去重
            HashSet<Long> idSet = new HashSet<>();
            for (UserBehavior value : iterable) {
                idSet.add(value.getUserId());
            }
            collector.collect(new PageViewCount("url",timeWindow.getEnd(),(long)idSet.size()));
        }
    }
}
