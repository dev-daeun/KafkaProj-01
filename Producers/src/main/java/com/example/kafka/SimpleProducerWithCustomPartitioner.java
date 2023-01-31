package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;


public class SimpleProducerWithCustomPartitioner {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {
        // KafkaProducer configuration settings
        Properties props = new Properties();

        // Producer settings 에 필요한 것
        // 1. broker 주소
        // 2. key, value serializer class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 커스텀 파티셔너로 파티셔닝 하도록 설정하기
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");

        props.setProperty("custom.specialKey0", "0");
        props.setProperty("custom.specialKey1", "1");

        // 메세지를 전송하는 프로듀서 객체 만들기 (메세지 key 타입은 string, value 타입은 string)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "topic-with-five-partitions";

        // 메세지 여러개 생성하기
        for (int seq = 0; seq < 20; seq++) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(seq), "this-is-" + String.valueOf(seq));
            producer.send(record, (RecordMetadata recordMetadata, Exception exception) -> {
                if (exception == null) {
                    System.out.println("### record metadata received ### \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "timestamp: " + recordMetadata.timestamp());
                }
                if (exception != null) {
                    System.out.println("exception from broker: " + exception.getMessage());
                }
            });
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
