package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class SimpleProducerSync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

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

        // 동기 방식으로 메세지 전송하기
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            logger.info("### record metadata received ### \n" +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset() + "\n" +
                    "timestamp: " + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
