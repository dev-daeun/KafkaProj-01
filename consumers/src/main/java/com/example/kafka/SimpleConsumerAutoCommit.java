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


public class SimpleConsumerAutoCommit {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {
        // SimpleProducer 에서 생산하는 메세지 소비.
        String topic = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // consumer group identifier 설정
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");

        // auto commit 설정 (default: auto.commit.interval.ms = 5000, enable.auto.commit = true)
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");

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

        pollAutocommit(consumer);

    }

    private static void pollAutocommit(KafkaConsumer<String, String> consumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record: records) {
                    logger.info("record_key:{}, record_value:{}, partition:{}, record's offset:{}", record.key(), record.value(), record.partition(), record.offset());
                }
                try {
                    logger.info("main tread is sleeping {}ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
