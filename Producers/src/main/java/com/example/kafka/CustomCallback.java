package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CustomCallback implements Callback {

    private int seq;

    // 생성자
    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            System.out.println(
                " seq: " + this.seq +
                " partition: " + recordMetadata.partition() +
                " offset: " + recordMetadata.offset() +
                " timestamp: " + recordMetadata.timestamp()
            );
        }
        else {
            System.out.println("exception from broker: " + exception.getMessage());
        }
    }
}
