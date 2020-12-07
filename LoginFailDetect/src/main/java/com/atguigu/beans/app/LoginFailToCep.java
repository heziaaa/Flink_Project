package com.atguigu.beans.app;

import com.atguigu.beans.LoginEvent;
import com.atguigu.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailToCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取文件
        DataStream<String> fileStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv");
        //转换成POJO
        DataStream<LoginEvent> dataStream = fileStream.map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(LoginEvent loginEvent) {
                return loginEvent.getTimestamp() * 1000L;
            }
        });
        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("FristFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .next("SecondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .within(Time.seconds(2));
        //2、在以userId分组之后的数据流上应用patten
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), loginEventPattern);

        //3、检出符合匹配条件的事件，得到一个新的DataStream
        SingleOutputStreamOperator<LoginFailWarning> resultStream = patternStream.select(new LoginFailMatchSelect());
        resultStream.print();
        env.execute("login fail to cep job");
    }

    private static class LoginFailMatchSelect implements PatternSelectFunction<LoginEvent,LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
            //获取第一次时间，最后一次时间
            Long userId = map.get("FristFail").get(0).getUserId();
            Long firstFailTs = map.get("FristFail").get(0).getTimestamp();
            Long secondFailTs = map.get("SecondFail").get(0).getTimestamp();
            return new LoginFailWarning(userId,firstFailTs,secondFailTs,"登录异常！！！");
        }
    }
}
