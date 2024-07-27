package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * kafka消费者工具类
 *
 * @author 孙宇
 */
public enum KafkaConsumerUtil implements Serializable, Closeable {
    INSTANCE;
    private Log log = LogFactory.get();
    private boolean keepConsuming = true;
    private Properties config = new Properties();
    private Consumer<String, String> consumer;
    private List<String> topics;

    /**
     * 设置kafka地址
     *
     * @param bootstrapServers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaConsumerUtil setBootstrapServers(String bootstrapServers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return INSTANCE;
    }

    /**
     * 设置消费者组
     *
     * @param groupId 组id
     * @return
     */
    public KafkaConsumerUtil setGroupId(String groupId) {
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return INSTANCE;
    }

    /**
     * 设置topic集合
     *
     * @param topics 主题集合
     * @return
     */
    public KafkaConsumerUtil setTopics(List<String> topics) {
        this.topics = topics;
        return INSTANCE;
    }

    /**
     * 设置topic
     *
     * @param topic 主题
     * @return
     */
    public KafkaConsumerUtil setTopics(String topic) {
        topics = Arrays.asList(topic);
        return INSTANCE;
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public KafkaConsumerUtil build() {
        //topics = Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB");
        //config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        //config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_sdk_kafka");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(config);
        consumer.subscribe(topics);
        return INSTANCE;
    }

    /**
     * 持续消费，一条条处理，如果不抛异常，则会自动提交offset
     *
     * @param callback 回调处理一条消息
     */
    public void pollRecord(ConsumerRecordCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            try {
                for (ConsumerRecord<String, String> record : records) {
                    callback.exec(record);//回调，由调用方处理消息
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));//记录消息偏移量+1
                    try {
                        consumer.commitSync(offsets);//提交偏移量
                    } catch (Exception e) {
                        //提交offset失败，跳出循环，重新获取消息
                        break;
                    }
                }
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    /**
     * 持续消费，一批批处理，如果不抛异常，则会自动提交这一批的offset
     *
     * @param pollTime 拉取消息等待时间(建议设置100毫秒)
     * @param callback 回调处理这一批消息
     */
    public void pollRecords(long pollTime, ConsumerRecordsCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = consumer.poll(pollTime);
            try {
                callback.exec(records);
                try {
                    consumer.commitSync();
                } catch (Exception e) {
                    //提交offset失败
                }
            } catch (Exception e) {
                //这批消息处理失败
                log.error(e);
            }
        }
    }


    /**
     * 调整偏移量
     *
     * @param topic     主题
     * @param partition 分区号
     * @param offset    偏移量
     */
    public void seek(String topic, int partition, long offset) {
        KafkaConsumer<Object, Object> tmpConsumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        tmpConsumer.assign(Arrays.asList(topicPartition));
        tmpConsumer.seek(topicPartition, offset);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        tmpConsumer.commitSync(offsets);
        tmpConsumer.close();
    }

    /**
     * 调整偏移量到LATEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToEnd(String topic, int partition) {
        KafkaConsumer<Object, Object> tmpConsumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        tmpConsumer.assign(Arrays.asList(topicPartition));
        tmpConsumer.seekToEnd(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = tmpConsumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        tmpConsumer.commitSync(offsets);
        tmpConsumer.close();
    }

    /**
     * 调整偏移量到EARLIEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToBeginning(String topic, int partition) {
        KafkaConsumer<Object, Object> tmpConsumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        tmpConsumer.assign(Arrays.asList(topicPartition));
        tmpConsumer.seekToBeginning(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = tmpConsumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        tmpConsumer.commitSync(offsets);
        tmpConsumer.close();
    }

    /**
     * 显示offset情况
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void showOffsets(String topic, int partition) {
        KafkaConsumer<Object, Object> tmpConsumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        tmpConsumer.assign(Arrays.asList(topicPartition));
        tmpConsumer.seekToBeginning(topicPartition);
        long offset = tmpConsumer.position(topicPartition);
        log.debug("EARLIEST offset {} {} {}", topic, partition, offset);
        OffsetAndMetadata committed = tmpConsumer.committed(topicPartition);
        long committedOffset = (committed != null) ? committed.offset() : -1;
        log.debug("current group offset {} {} {}", topic, partition, committedOffset);
        tmpConsumer.seekToEnd(topicPartition);
        offset = tmpConsumer.position(topicPartition);
        log.debug("LATEST offset {} {} {}", topic, partition, offset);
        tmpConsumer.close();
    }

    /**
     * 显示主题的所有分区信息
     *
     * @param topic 主题
     */
    public void showPartitions(String topic) {
        KafkaConsumer<Object, Object> tmpConsumer = new KafkaConsumer<>(config);
        List<PartitionInfo> partitionInfos = tmpConsumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
            log.debug("{}", partitionInfo);
        }
        tmpConsumer.close();
    }

    /**
     * 停止消费，释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() {
        keepConsuming = false;
        try {
            consumer.close();
        } catch (Exception e) {
            log.error(e);
        }
    }
}