package com.atguigu.app;

import com.atguigu.beans.ApacheLogEvent;
import com.atguigu.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class HotPages {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\NetworkFlow\\src\\main\\resources\\apache.log");

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            //对时间进行转换得到时间戳
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timeStamp = sdf.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timeStamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTime();
            }
        });

        //定义一个测输出流的标签，用于存放迟到数据
        OutputTag<ApacheLogEvent> lateOutputTag = new OutputTag<ApacheLogEvent>("late"){};

        //开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))  //允许处理迟到数据，窗口延迟关闭
                .sideOutputLateData(lateOutputTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        //排序输出
        DataStream<String> resultStream = windowAggStream.keyBy("windowEnd")
                .process(new MyProcessFunction(5));

        //获取测输出流
        windowAggStream.getSideOutput(lateOutputTag).print("late");

        resultStream.print("hot page count");

        env.execute();
    }


    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
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
            return acc1 + aLong;
        }
    }

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(s, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    private static class MyProcessFunction extends ProcessFunction<PageViewCount, String> {
        private Integer topNSize;

        public MyProcessFunction(Integer i) {
            this.topNSize = i;
        }

        //定义一个状态
        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //给状态赋值
            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("pageTimeViewCountList", PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            //将数据添加到状态中
            pageViewCountListState.add(pageViewCount);
            //创建一个定时器
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发定时器，说明所有数据都到齐了，排序输出。

            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            //排序
            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    //      return o2.getCount().intValue() - o1.getCount().intValue();
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
            //将排名信息格式化成字符串输出，
            StringBuilder result = new StringBuilder();
            result.append("===========================\n");
            result.append("窗口的开始时间：").append(new Timestamp(timestamp - 1));

            //遍历list
            for (int i = 0; i < Math.min(topNSize, pageViewCounts.size()); i++) {
                PageViewCount currentPageViewCount = pageViewCounts.get(i);
                result.append("NO").append(i + 1).append(":")
                        .append("url = ").append(currentPageViewCount.getUrl())
                        .append("热门度 = ").append(currentPageViewCount.getCount())
                        .append("\n");
            }
            result.append("===========================\n");
            Thread.sleep(1000);
            out.collect(result.toString());
        }
    }
}
