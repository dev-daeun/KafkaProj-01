package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.util.Properties;


public class SimpleProducerAsync {
//    public static final Logger  logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {
        // KafkaProducer configuration settings
        Properties props = new Properties();

        // Producer settings 에 필요한 것
        // 1. broker 주소
        // 2. key, value serializer class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 메세지를 전송하는 프로듀서 객체 만들기 (메세지 key 타입은 string, value 타입은 string)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // 메세지 객체 생성하기 (key 타입은 string, value 타입은 string)
        String topic = "simple-topic";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "this-is-key", "this-is-value");

        // 비동기 방식으로 메세지 전송하기
        producer.send(record, (RecordMetadata recordMetadata, Exception exception) -> {
            // 메세지 전송 및 ACK 처리를 담당하는 서브쓰레드에서 수행하는 콜백함수
            if (exception == null) {
                System.out.println("### record metadata received ### \n" +
                        "partition: " + recordMetadata.partition() + "\n" +
                        "offset: " + recordMetadata.offset() + "\n" +
                        "timestamp: " + recordMetadata.timestamp());
            }
            else {
                System.out.println("exception from broker: " + exception.getMessage());
            }
        });

        // 콜백함수가 실행될 때까지 대기
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
