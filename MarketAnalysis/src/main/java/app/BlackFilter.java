package app;

import beans.AdClickEvent;
import beans.BlackListWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BlackFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> fileStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv");
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
        //分组，统计
        SingleOutputStreamOperator<AdClickEvent> filterAdClientStream = dataStream.keyBy("userId", "adId")
                .process(new UserIdAndAdIDCount(100));

        //获得测输出流
        filterAdClientStream.getSideOutput(new OutputTag<BlackListWarning>("blackList"){}).print("black list");
        env.execute();
    }

    private static class UserIdAndAdIDCount extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
        private Integer countUpperBound;

        public UserIdAndAdIDCount(Integer i) {
            this.countUpperBound = i;
        }

        //定义状态,记录当前用户对某个广告的点击次数
        ValueState<Long> countState;
        //定义状态，用来标记当前用户是否已经发送到黑名单
        ValueState<Boolean> isSendState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class,0L));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSend", Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {
            //获取当前count值；
            Long curCount = countState.value();
            //判断如果时第一条数据，那么就注册定时器，每天0点清空
            if (curCount == 0) {
                //获得每天0点时间
                Long ts = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000L) - (8 * 60 * 60 * 1000L);
                //注册定时器
                context.timerService().registerProcessingTimeTimer(ts);
            }
            //判断如果count值超过上限，就将数据发送给侧输出流
            if (curCount >= countUpperBound){
                //判断该黑名单是否已经在测输出流中了,没有则输出到测输出流中
                if (!isSendState.value()){
                    context.output(new OutputTag<BlackListWarning>("blackList"){},new BlackListWarning(adClickEvent.getUserId(),adClickEvent.getAdId(),"报警，异常点击"));
                    //并且将issendState状态改为true
                    isSendState.update(true);
                }
                return;
            }
            //如果没有达到上限，则count + 1
            countState.update(curCount + 1);
            collector.collect(adClickEvent);
        }

        //触发定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            //清空状态
            countState.clear();
            isSendState.clear();
        }
    }
}
