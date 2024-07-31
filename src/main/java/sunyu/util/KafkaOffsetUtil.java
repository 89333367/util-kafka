package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.Serializable;
import java.util.*;

/**
 * kafka偏移量工具类
 *
 * @author 孙宇
 */
public enum KafkaOffsetUtil implements Serializable, Closeable {
    INSTANCE;
    private Log log = LogFactory.get();

    /**
     * 获取工具类工厂
     *
     * @return
     */
    public static KafkaOffsetUtil builder() {
        return INSTANCE;
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public KafkaOffsetUtil build() {
        //topics = Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB");
        //config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        //config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_sdk_kafka");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return INSTANCE;
    }

    @Override
    public void close() {
    }


    private Properties config = new Properties();

    /**
     * 设置kafka地址
     *
     * @param bootstrapServers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaOffsetUtil setBootstrapServers(String bootstrapServers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return INSTANCE;
    }

    /**
     * 设置消费者组
     *
     * @param groupId 组id
     * @return
     */
    public KafkaOffsetUtil setGroupId(String groupId) {
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return INSTANCE;
    }


    /**
     * 调整偏移量
     *
     * @param topic     主题
     * @param partition 分区号
     * @param offset    偏移量
     */
    public void seek(String topic, int partition, long offset) {
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offset);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
        consumer.close();
    }

    /**
     * 调整偏移量到LATEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToEnd(String topic, int partition) {
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = consumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
        consumer.close();
    }

    /**
     * 调整偏移量到EARLIEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToBeginning(String topic, int partition) {
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = consumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
        consumer.close();
    }

    /**
     * 显示offset情况
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void showOffsets(String topic, int partition) {
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(topicPartition);
        long offset = consumer.position(topicPartition);
        log.debug("EARLIEST offset {} {} {}", topic, partition, offset);
        OffsetAndMetadata committed = consumer.committed(topicPartition);
        long committedOffset = (committed != null) ? committed.offset() : -1;
        log.debug("current group offset {} {} {}", topic, partition, committedOffset);
        consumer.seekToEnd(topicPartition);
        offset = consumer.position(topicPartition);
        log.debug("LATEST offset {} {} {}", topic, partition, offset);
        consumer.close();
    }

    /**
     * 显示主题的所有分区信息
     *
     * @param topic 主题
     */
    public void showPartitions(String topic) {
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
            log.debug("{}", partitionInfo);
        }
        consumer.close();
    }

}