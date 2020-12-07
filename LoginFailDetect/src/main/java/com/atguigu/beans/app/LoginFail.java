package com.atguigu.beans.app;

import com.atguigu.beans.LoginEvent;
import com.atguigu.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class LoginFail {
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
        SingleOutputStreamOperator<LoginFailWarning> resultStream = dataStream.keyBy(LoginEvent::getUserId)
                .process(new MyProcessFunc(2));

        resultStream.print();

        env.execute("login fail detect job");

    }

    private static class MyProcessFunc extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        private Integer failUpperBound;

        public MyProcessFunc(Integer failUpperBound) {
            this.failUpperBound = failUpperBound;
        }

        //定义一个状态，保存loginEvent 信息
        ListState<LoginEvent> loginEventListState;
        //定义状态保存定时器的时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginEventState", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
        }


        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            //判断事件是成功还是失败
            if ("fail".equals(loginEvent.getEventType())) {
                //根据List里已有的登录失败做判断

                //Step2.获取当前定时器的状态，看有没有失败的事件
                Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
                if (iterator.hasNext()) {
                    //如果有失败的事件，进一步判断两者的关系
                    LoginEvent event = iterator.next();
                    //两个失败事件的时间在两秒内
                    if (loginEvent.getTimestamp() - event.getTimestamp() <= 2) {
                        //输出报警信息
                        collector.collect(new LoginFailWarning(loginEvent.getUserId(), event.getTimestamp(), loginEvent.getTimestamp(), "登录异常！！！"));
                    }
                    //不管报不报警，当前已经处理完毕，清空状态，将最近一次登录失败事件保存进list
                    loginEventListState.clear();
                    loginEventListState.add(loginEvent);
                } else {
                    //状态中没有事件，之间添加进状态
                    loginEventListState.add(loginEvent);
                }

            } else {
                //如果事件为成功，则直接清空状态
                loginEventListState.clear();
            }
            //没来一条数据判断是不是fail
            /*if ("fail".equals(loginEvent.getEventType())) {
                //添加到List中
                loginEventListState.add(loginEvent);
                //判断是否有定时器，如果没有则创建定时器
                if (timerTsState.value() == null) {
                    Long ts = (loginEvent.getTimestamp() + 2) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                //如果是成功事件，则删除定时器，清空状态,可能第一条数据就是成功数据，判断一下
                if (timerTsState.value() != null) {
                    context.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                timerTsState.clear();
                loginEventListState.clear();*/
        }
    }
       /* @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            //执行定时器,判断状态中的不成功事件是否大于2
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginEventListState.get().iterator());
            if (loginEvents.size() >= 2){
                //输出报警信息
                LoginEvent loginEvent = loginEvents.get(0);
                out.collect(new LoginFailWarning(loginEvent.getUserId(),loginEvent.getTimestamp(),
                        loginEvents.get(loginEvents.size()-1).getTimestamp(),"连续登录失败异常！"));
            }
            //最后删除定时器，清空状态，重新开始

            loginEventListState.clear();
            timerTsState.clear();
        }*/
//}
}

