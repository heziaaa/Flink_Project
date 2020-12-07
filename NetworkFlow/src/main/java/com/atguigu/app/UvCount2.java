package com.atguigu.app;

import com.atguigu.beans.PageViewCount;
import com.atguigu.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class UvCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1、读取数据
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
        DataStream<PageViewCount> resultStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger()) //触发器，触发窗口执行并关闭
                .process(new MyProcessFunction());
        resultStream.print();
        env.execute();
    }

    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            //没来一条数据，直接触发窗口计算，并清空计算
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    //创建一个布隆过滤器
    public static class MyBloomFilter {
        //定义布隆过滤器的总容量，bit的个数，必须是2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        //创建hash算法
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            //返回result，不能超过cap
            return result & (cap - 1);
        }
    }

    private static class MyProcessFunction extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        //需要写入Redis中
        Jedis redis;
        MyBloomFilter myBloomFilter;
        // 定义redis中保存uv count值的hash表名称
        final String uvCountMapName = "uvCount";

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建redis连接
            redis = new Jedis("hadoop100",6379);
            myBloomFilter = new MyBloomFilter(1 << 29); //2^29次幂，64MB
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            //redis中相关key的定义
            String bitMapKey = String.valueOf(context.window().getEnd());
            String uvCountKey = String.valueOf(context.window().getEnd());
            //取出iterable中userId
            String userId = iterable.iterator().next().getUserId().toString();
            //获得偏移量
            Long offset = myBloomFilter.hashCode(userId, 67);
            //判断offset的值是否为0
            Boolean isExist = redis.getbit(bitMapKey, offset);
            if (!isExist) {
                //为零就改为1
                redis.setbit(bitMapKey, offset, true);
            }
            //获取当前的count值，保存
            Long count = 0L;
            String uvCountString = redis.hget(uvCountMapName, uvCountKey);
            if (uvCountString != null && !"".equals(uvCountString)) {
                count = Long.valueOf(uvCountString);
            }
            redis.hset(uvCountMapName, uvCountKey, String.valueOf(count + 1));

            collector.collect(new PageViewCount("url", context.window().getEnd(), count + 1));
        }

        @Override
        public void close() throws Exception {
            redis.close();

        }
    }
}
