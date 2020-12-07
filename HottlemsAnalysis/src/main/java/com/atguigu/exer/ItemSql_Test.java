package com.atguigu.exer;

import com.atguigu.beans.UserBehavior;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class ItemSql_Test {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取kafka数据
//        DataStream<String> inputStream = env.readTextFile("D:\\JavaStudy\\Flimk_Project\\HottlemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop100:9092");
        properties.setProperty("group.id","consumer_Flink");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        //转换数据为POJO
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] words = line.split(",");
            return new UserBehavior(new Long(words[0]), new Long(words[1]), new Integer(words[2]), words[3], new Long(words[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {//获取事件时间，有序数据

            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
        //4、创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId,behavior,timestamp.rowtime as ts");
        tableEnv.createTemporaryView("data_Table", dataTable);
        //使用table API过滤，分组，开窗，聚合
        Table windowAggTable = dataTable.filter("behavior='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as count");
        //使用 sql 进行过滤，分组，开窗，聚合
        String sql = "select itemId,tumble_end(ts,interval '1' hour) as windowEnd,count(itemId) as cnt " +
                "from data_Table where behavior = 'pv' " +
                "group by itemId,tumble(ts, interval '1' hour)";
        Table aggSqlTable = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("aggTable",aggSqlTable);
        //最后求TopN
        String topnSql = "select *" +
                "from (select *,row_number() over(partition by windowEnd order by cnt desc) as row_num from aggTable )" +
                "where row_num <= 5";
        Table resultTable = tableEnv.sqlQuery(topnSql);
        //转换为流，进行输出
        tableEnv.toRetractStream(resultTable, Row.class).print("sql");

        env.execute("hot items with sql job");
    }
}
