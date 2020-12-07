package com.atguigu.app;

import com.atguigu.beans.OrderEvent;
import com.atguigu.beans.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

public class TxPayMatchByJoin {
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

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchDelectJoin());

        resultStream.print("orderPay and TxPay Match success");
        env.execute();
    }

    private static class TxPayMatchDelectJoin extends ProcessJoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
                collector.collect(new Tuple2<>(orderEvent,receiptEvent));
        }
    }
}
