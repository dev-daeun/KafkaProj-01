package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class SimpleConsumerManualCommit {

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
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-05");

        // manual commit 설정
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

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

//        pollCommitSync(consumer);
        pollCommitAsync(consumer);

    }

    private static void pollCommitAsync(KafkaConsumer<String, String> consumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record: records) {
                    logger.info("record_key:{}, record_value:{}, partition:{}, record's offset:{}", record.key(), record.value(), record.partition(), record.offset());
                }
                try {
                    // record 순회할 때마다 커밋 하는 게 아니라 batch 단위로 커밋.
                    // 비동기 방식은 커밋 결과를 콜백으로 전달함.
                    if(records.count() > 0) {
                        consumer.commitAsync(new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                if (exception != null) {
                                    logger.error("offset {} is not completed. error: {}", offsets, exception);
                                }
                                else {
                                    logger.info("offset {} has been committed successfully.", offsets);
                                }
                            }
                        });
                        logger.info("commitAsync has been called.");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has occurred.");
        } finally {
            logger.info("kafka consumer is closing...");
            consumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> consumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord record: records) {
                    logger.info("record_key:{}, record_value:{}, partition:{}, record's offset:{}", record.key(), record.value(), record.partition(), record.offset());
                }
                try {
                    // record 순회할 때마다 커밋 하는 게 아니라 batch 단위로 커밋.
                    if(records.count() > 0) {
                        consumer.commitSync();
                        logger.info("commitSync has been called.");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
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
