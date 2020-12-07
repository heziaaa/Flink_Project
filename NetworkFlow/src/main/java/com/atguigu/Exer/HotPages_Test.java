package com.atguigu.Exer;

import com.atguigu.beans.ApacheLogEvent;
import com.atguigu.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class HotPages_Test {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取文件数据
//        DataStream<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\NetworkFlow\\src\\main\\resources\\apache.log");
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop100", 7777);

        //转换为pojo
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            //转换时间，得到时间戳
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timestamp = sdf.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTime();
            }
        });
        //4、对dataStream开窗聚合操作，得到pageViewCount。
        //给测输出流一个标签
        OutputTag<ApacheLogEvent> outputTag = new OutputTag<ApacheLogEvent>("last") {
        };
        SingleOutputStreamOperator<PageViewCount> pageViewCountDataStream = dataStream.filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1)) //允许迟到一分钟数据
                .sideOutputLateData(outputTag) //测输出流保留迟到数据
                .aggregate(new MyAggFunction(), new MyWindowFunction());

        //排序输出
        pageViewCountDataStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPage(5))
                .print("hot page count");
        //获取测输出流，打印输出。
        pageViewCountDataStream.getSideOutput(outputTag).print("测输出流");

        env.execute();
    }

    private static class MyAggFunction implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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

    //获取window信息，保存到pageViewCount中。
    private static class MyWindowFunction implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(s, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    private static class TopNHotPage extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topNSize;

        public TopNHotPage(Integer i) {
            this.topNSize = i;
        }

        //声明一个状态,用于保存viewCount,因为我们可能有迟到数据到来，所以需要对数据进行修改，所以选用了MapState
        MapState<String, Long> pageViewCountState;


        @Override
        public void open(Configuration parameters) throws Exception {
            //为pageCountsTATE赋值
            pageViewCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("pageCountState", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            //保存数据
            pageViewCountState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            //创建一个定时器
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            //创建一个定时器，关闭状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60000L);
        }

        //触发定时器，对数据排序
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //执行关闭状态
            if (timestamp == ctx.getCurrentKey() + 60000L){
                pageViewCountState.clear();
                return;
            }

            //转成list，排序
            ArrayList<Map.Entry<String, Long>> entries = Lists.newArrayList(pageViewCountState.entries().iterator());
            entries.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue()) {
                        return -1;
                    } else if (o1.getValue() < o2.getValue()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
            //将排名信息格式化成string ，输出Topn
            StringBuilder result = new StringBuilder();
            result.append("=============================\n");
            result.append("窗口结束时间：").append(ctx.getCurrentKey()).append("\n");
            for (int i = 0; i < Math.min(topNSize, entries.size()); i++) {
                //获得数据
                Map.Entry<String, Long> entry = entries.get(i);
                result.append("NO").append(i + 1).append(": ")
                        .append("页面URL=").append(entry.getKey())
                        .append("浏览量=").append(entry.getValue())
                        .append("\n");
            }
            result.append("=============================\n");
            Thread.sleep(1000);
            out.collect(result.toString());

        }
    }
}
