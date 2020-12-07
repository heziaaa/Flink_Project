package com.atguigu.exer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer {
    public static void main(String[] args) throws IOException {
        myKafka("hotitems");
    }

    //kafka生产者，发送消息
    public static void myKafka(String topic) throws IOException {
        //创建一个kafkaproducer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //读取文件，发送数据
        BufferedReader reader = new BufferedReader(new FileReader("D:\\JavaStudy\\Flimk_Project\\HottlemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        //循环读取所有数据
        String line;
        while ((line = reader.readLine()) != null){
            //发送给kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            kafkaProducer.send(record);
        }
        //关闭
        kafkaProducer.close();
    }
}
