package com.atguigu.app;

import com.atguigu.beans.OrderEvent;
import com.atguigu.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxPayMatch {
    //定义两个测输出流标签
    private final static OutputTag<OrderEvent> unmatchPayOutTag = new OutputTag<OrderEvent>("unmatchPayOut") {
    };
    private final static OutputTag<ReceiptEvent> unmatchReceiptOutTag = new OutputTag<ReceiptEvent>("unmatchReceiptOut") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取文件
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        URL txPayResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                }).filter(data -> "pay".equals(data.getEventType()));
        DataStream<ReceiptEvent> receiptEventStream = env.readTextFile(txPayResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent receiptEvent) {
                        return receiptEvent.getTimestamp() * 1000L;
                    }
                });

        //使用connect 将两条流合并
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new OrderMatchPayFunc());

        //输出
        resultStream.print("order and receipt is ok");
        resultStream.getSideOutput(unmatchPayOutTag).print("receipt is error");
        resultStream.getSideOutput(unmatchReceiptOutTag).print("order pay is error");


        env.execute();

    }

    private static class OrderMatchPayFunc extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        //定义两个状态，保存orderEvent,和ReceiptEvent
        ValueState<OrderEvent> orderEventValueState;
        ValueState<ReceiptEvent> receiptEventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-event-state", OrderEvent.class));
            receiptEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-event-state", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            if (receiptEventValueState.value() != null) {
                //已经有到账事件，输出到主流
                collector.collect(new Tuple2<>(orderEvent, receiptEventValueState.value()));
                //清空状态
                receiptEventValueState.clear();
            } else {
                //receiptEvent 没来，等待支付事件5s 创建的定时器
                Long ts = (orderEvent.getTimestamp() + 5) * 1000L;
                context.timerService().registerEventTimeTimer(ts);
                //保存状态
                orderEventValueState.update(orderEvent);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            if (orderEventValueState.value() != null) {
                //已有支付事件，输出到主流
                collector.collect(new Tuple2<>(orderEventValueState.value(), receiptEvent));
                //清空状态
                orderEventValueState.clear();
            } else {
                //没有支付事件，等待到账事件3s 创建定时器
                Long ts = (receiptEvent.getTimestamp() + 3) * 1000L;
                context.timerService().registerEventTimeTimer(ts);
                //保存状态
                receiptEventValueState.update(receiptEvent);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //执行定时器,判断状态中是否有数据
            if (orderEventValueState.value() != null) {
                //等待到账事件，到账事件没来
                ctx.output(unmatchPayOutTag, orderEventValueState.value());
            }
            if (receiptEventValueState.value() != null) {
                ctx.output(unmatchReceiptOutTag, receiptEventValueState.value());
            }
            //清空状态
            orderEventValueState.clear();
            //清空状态
            receiptEventValueState.clear();
        }
    }
}
