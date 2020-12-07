package app;

import beans.AdClickEvent;
import beans.AdCountByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;

public class PageAdByProvinceCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
//        URL resource = PageAdByProvinceCount.class.getResource("/AdClickLog.csv");
//        DataStream<String> fileStream = env.readTextFile(resource.getPath());
        DataStreamSource<String> fileStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv");

        //转换为POJO
        DataStream<AdClickEvent> dataStream = fileStream.map(line -> {
            String[] fileds = line.split(",");
            return new AdClickEvent(new Long(fileds[0]), new Long(fileds[1]), fileds[2], fileds[3], new Long(fileds[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent adClickEvent) {
                return adClickEvent.getTimestamp() * 1000L;
            }
        });
        //过滤，开窗，聚合
        DataStream<AdCountByProvince> adCountByProvinceStream = dataStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdCountByProvinceAgg(), new WindowCount());

        adCountByProvinceStream.print();
/*        adCountByProvinceStream
                .keyBy(AdCountByProvince::getWindowEnd)
                .process(new MyProcessFunc());*/

        env.execute();
    }

    private static class AdCountByProvinceAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
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

    private static class WindowCount implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountByProvince> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            collector.collect(new AdCountByProvince(s, windowEnd, iterable.iterator().next()));
        }
    }
}
