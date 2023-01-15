package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class SimpleProducerAsyncWithCustomCallback {
//    public static final Logger  logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {
        // KafkaProducer configuration settings
        Properties props = new Properties();

        // Producer settings 에 필요한 것
        // 1. broker 주소
        // 2. key, value serializer class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 메세지를 전송하는 프로듀서 객체 만들기 (메세지 key 타입은 integer, value 타입은 string)
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        String topic = "topic-with-multi-partitions";

        // 메세지 여러개 생성하기
        for (int seq = 0; seq < 20; seq++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, seq, "this-is-" + seq);
            Callback callback = new CustomCallback(seq);
            producer.send(record, callback);
        }

        // 콜백함수가 실행될 때까지 대기
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}

/*
실행결과
 seq: 1 partition: 0 offset: 19 timestamp: 1673781314802
 seq: 7 partition: 0 offset: 20 timestamp: 1673781314802
 seq: 8 partition: 0 offset: 21 timestamp: 1673781314802
 seq: 14 partition: 0 offset: 22 timestamp: 1673781314802
 seq: 15 partition: 0 offset: 23 timestamp: 1673781314802
 seq: 17 partition: 0 offset: 24 timestamp: 1673781314802
 seq: 0 partition: 1 offset: 22 timestamp: 1673781314792
 seq: 3 partition: 1 offset: 23 timestamp: 1673781314802
 seq: 4 partition: 1 offset: 24 timestamp: 1673781314802
 seq: 9 partition: 1 offset: 25 timestamp: 1673781314802
 seq: 10 partition: 1 offset: 26 timestamp: 1673781314802
 seq: 11 partition: 1 offset: 27 timestamp: 1673781314802
 seq: 13 partition: 1 offset: 28 timestamp: 1673781314802
 seq: 2 partition: 2 offset: 19 timestamp: 1673781314802
 seq: 5 partition: 2 offset: 20 timestamp: 1673781314802
 seq: 6 partition: 2 offset: 21 timestamp: 1673781314802
 seq: 12 partition: 2 offset: 22 timestamp: 1673781314802
 seq: 16 partition: 2 offset: 23 timestamp: 1673781314802
 seq: 18 partition: 2 offset: 24 timestamp: 1673781314802
 seq: 19 partition: 2 offset: 25 timestamp: 1673781314802
* */