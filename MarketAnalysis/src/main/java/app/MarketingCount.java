package app;

import beans.MarketUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MarketingCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        DataStream<MarketUserBehavior> marketUserStream = env.addSource(new SourceFunction<MarketUserBehavior>() {
            //定一个flag，用于跳出循环
            Boolean flag = true;
            Random random = new Random();
            //定义用户行为和推广渠道
            List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
            List<String> channelList = Arrays.asList("app store", "wechat", "weibo", "tieba");

            @Override
            public void run(SourceContext<MarketUserBehavior> sourceContext) throws Exception {
                while (flag) {
                    //随机生成字段
                    Long id = random.nextLong();
                    String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                    String channel = channelList.get(random.nextInt(channelList.size()));
                    Long timestamp = System.currentTimeMillis();
                    sourceContext.collect(new MarketUserBehavior(id, behavior, channel, timestamp));
                    Thread.sleep(100L);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketUserBehavior>() {
            @Override
            public long extractAscendingTimestamp(MarketUserBehavior marketUserBehavior) {
                return marketUserBehavior.getTimestamp();
            }
        });
        SingleOutputStreamOperator<String> resultStream = marketUserStream.filter(data -> "UNINSTALL".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketCountAgg(), new MarketCountWindow());
        resultStream.print("total count");
        env.execute();
    }

    private static class MarketCountAgg implements AggregateFunction<MarketUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior marketUserBehavior, Long aLong) {
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

    private static class MarketCountWindow extends ProcessAllWindowFunction<Long, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = iterable.iterator().next();
            String result = windowEnd + "," + count.toString();
            collector.collect(result);
        }
    }
}
