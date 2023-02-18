package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class SimpleConsumerSeekOffset {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {
        // SimpleProducer 에서 생산하는 메세지 소비.
        String topic = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // consumer group identifier 설정
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-06");

        // auto commit 하지 않도록 설정
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // offset 5번부터 읽어들이기로 함.
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition, 5);

        // 별도 쓰레드에서 client.wakeup() 호출 -> 메인쓰레드에서 WakeupException 발생
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord record : records) {
                logger.info("record_key:{}, record_value:{}, partition:{}, record's offset:{}", record.key(), record.value(), record.partition(), record.offset());
            }

        }
    }
}
