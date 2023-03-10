package com.example.kafka;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumerHearbeatThreadConf {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumerHearbeatThreadConf.class.getName());

    public static void main(String[] args) {
        String topic = "topic-with-five-partitions";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // consumer group identifier 설정
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");

        // heartbeat 관련 설정
        // heartbeat thread 가 브로커에게 hearbeat 날리는 주기
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5");

        // 브로커에서 heartbeat 을 기다리는 최대 시간 -> 타임아웃 되면 컨수머그룹 리밸런생 발생
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "50000");

        // 브로커에서 컨수머의 polling 을 기다리는 최대 시간 -> 타임아웃 되면 컨수머그룹 리밸런싱 발생
        // 60초로 설정.
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // consumer group 은 여러 개의 토픽 구독이 가능함.
        consumer.subscribe(List.of(topic));

        int loopCnt = 0;
        while (true) {
            // MAX_POLL_INTERVAL_MS_CONFIG 보다 시간이 더 걸리는 polling 처리. -> 리밸런싱 대상.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("####### loopCnt: {} consumerRecords count: {}", loopCnt++, records.count());
            for (ConsumerRecord record: records) {
                logger.info("record_key:{}, record_value:{}, partition:{}", record.key(), record.value(), record.partition());
            }
            try {
                logger.info("main thread is sleeping {} ms during while loop ", loopCnt * 10000);
                Thread.sleep(loopCnt * 10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
