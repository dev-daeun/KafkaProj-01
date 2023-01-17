package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private ArrayList<String> specialKeyNames = new ArrayList<String>();

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

        // partitionsForSpecialKey = (int)(2.5) = 2
        int partitionsForSpecialKey = (int)(partitionsAmount * 0.5);

        int partitionIndex;
        if (this.specialKeyNames.contains(key.toString())) {
            // specialKey 메세지들은  0, 1 인덱스에 해당하는 파티션들로 분산
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % partitionsForSpecialKey;
        }
        else {
            // specialKey 아닌 메세지들은 2, 3, 4 인덱스에 해당하는 파티션들로 분산
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (partitionsAmount - partitionsForSpecialKey) + partitionsForSpecialKey;
        }

        System.out.println("key " + key.toString() + " is sent to partition " + partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.specialKeyNames.add(configs.get("custom.specialKey0").toString());
        this.specialKeyNames.add(configs.get("custom.specialKey1").toString());
    }
}
