package com.atguigu.app;

import com.atguigu.beans.OrderEvent;
import com.atguigu.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class OrderTimeOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取数据
        URL resource = OrderTimeOut.class.getResource("/OrderLog.csv");
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

        //1、定义一个模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                }).within(Time.minutes(15));

        //2、将模式应用到数据流上
        PatternStream<OrderEvent> orderEventPatternStream = CEP.pattern(dataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        //3、定义一个测输出流标签，用于标记超时订单
        OutputTag<OrderResult> orderTimeOutTag = new OutputTag<OrderResult>("orderTimeout") {
        };

        //4、调用select 方法，得到对应的订单结果，正常匹配的输出到主流，超时的输出到测输出流
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventPatternStream.select(orderTimeOutTag, new OrderTimeoutSelect(),new orderPayPatternSelect());

        //5、打印输出
        resultStream.print("payed");
        resultStream.getSideOutput(orderTimeOutTag).print("order pay timeout");

        env.execute("order timeout detect job");
    }

    //自定义超时拣选函数
    private static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();

            return new OrderResult(timeoutOrderId,"timeout");
        }
    }

    private static class orderPayPatternSelect implements PatternSelectFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payOrderId,"payed result");
        }
    }
}
