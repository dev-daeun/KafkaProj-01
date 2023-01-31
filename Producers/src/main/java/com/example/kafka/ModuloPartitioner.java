package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class ModuloPartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(ModuloPartitioner.class.getName());

    @Override
    // keyBytes: 직렬화된 키 값
    // valueBytes: 직렬화된 밸류 값
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (keyBytes == null) {
            throw new InvalidRecordException("key should not be null!");
        }

        // cluster.partitionsForTopic(topic): 토픽이 가지고 있는 파티션들의 정보를 리스트 타입으로 리턴
        // 예. 파티션 수 = 5 -> partitionsAmount = 5
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int partitionsAmount = partitionInfoList.size();

        int partitionIndex = Integer.parseInt(key.toString()) % partitionsAmount;

        logger.info("key " + key.toString() + " is sent to partition " + partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
