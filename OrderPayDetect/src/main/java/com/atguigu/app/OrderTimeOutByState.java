package com.atguigu.app;

import com.atguigu.beans.OrderEvent;
import com.atguigu.beans.OrderResult;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import javax.smartcardio.CardTerminal;
import java.net.URL;

public class OrderTimeOutByState {
    //定义一个outputTag
    private final static OutputTag<OrderResult> orderTimeOutTag = new OutputTag<OrderResult>("orderTimeOut"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        URL resource = OrderTimeOutByState.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> dataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                });
        //状态编程，订单取消
        SingleOutputStreamOperator<OrderResult> resultStream = dataStream.keyBy(OrderEvent::getOrderId)
                .process(new OrderTimeOutFunc());

        resultStream.print("主流");
        resultStream.getSideOutput(orderTimeOutTag).print("测流");

        env.execute();

    }

    private static class OrderTimeOutFunc extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        //定义状态
        ValueState<Boolean> orderTypeIsCreate;
        ValueState<Boolean> orderTypeIsPay;
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderTypeIsCreate = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("order-create", Boolean.class,false));
            orderTypeIsPay = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("order-pay", Boolean.class,false));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }


        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
            //判断当前数据类型，如果为create
            if ("create".equals(orderEvent.getEventType())) {
                //判断paystate中是否有数据
                if (orderTypeIsPay.value()) {
                    //pay也来了，输出
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "payed"));
                    //清空状态
                    orderTypeIsCreate.clear();
                    orderTypeIsPay.clear();
                    timerState.clear();
                    context.timerService().deleteEventTimeTimer(timerState.value());
                } else {
                    //paystate中没有数据，创建定时器
                    Long ts = (orderEvent.getTimestamp() + 15 * 60) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    orderTypeIsCreate.update(true);
                    timerState.update(ts);
                }
            } else if ("pay".equals(orderEvent.getEventType())) {
                //类型为pay，判断orderCreate中是否有数据
                if (orderTypeIsCreate.value()) {
                    //判断是否超过15min中
                    if (orderEvent.getTimestamp() * 1000L < timerState.value()) {
                        //没有超时，正常匹配
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed"));
                    } else {
                        //超过15min中，输出超时
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "time out"));
                    }
                    //删除定时器
                    context.timerService().deleteEventTimeTimer(timerState.value());
                    //清空状态
                    orderTypeIsCreate.clear();
                    orderTypeIsPay.clear();
                    timerState.clear();

                } else {
                    //创建定时器
                    Long ts = (orderEvent.getTimestamp()) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    timerState.update(ts);
                    orderTypeIsPay.update(true);
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //触发定时器，判断状态中是否有数据，确定是哪个地方
            if (orderTypeIsCreate.value()){
                //create中有数据，pay出问题
                ctx.output(orderTimeOutTag,new OrderResult(ctx.getCurrentKey(),"not create"));
            }else if(orderTypeIsPay.value()){
                ctx.output(orderTimeOutTag,new OrderResult(ctx.getCurrentKey(),"timeout"));
            }
            //清空状态
            orderTypeIsPay.clear();
            orderTypeIsCreate.clear();
            timerState.clear();
        }
    }
}
