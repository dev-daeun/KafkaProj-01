package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class SimpleConsumerWakeup {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {
        // SimpleProducerWithModuloPartitioner 에서 생산하는 메세지 소비.
        String topic = "topic-with-five-partitions";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // consumer group identifier 설정
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // consumer group 은 여러 개의 토픽 구독이 가능함.
        consumer.subscribe(List.of(topic));

        // 별도 쓰레드에서 client.wakeup() 호출 -> 메인쓰레드에서 WakeupException 발생
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("");
                consumer.wakeup();
                try { mainThread.join(); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
        });

        // shutdown kafka consumption gracefully...
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record: records) {
                    logger.info("record_key:{}, record_value:{}, partition:{}, record's offset:{}", record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has occurred.");
        } finally {
            logger.info("kafka consumer is closing...");
            consumer.close();
        }

    }
}
