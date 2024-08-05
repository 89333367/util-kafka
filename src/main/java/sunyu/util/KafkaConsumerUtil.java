package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka消费者工具类
 *
 * @author 孙宇
 */
public class KafkaConsumerUtil implements Serializable, Closeable {
    private Log log = LogFactory.get();

    private boolean keepConsuming = true;//持续消费
    private Properties config = new Properties();//消费者配置参数
    private Consumer<String, String> consumer;//消费者
    private List<String> topics;//消费主题集合
    private volatile boolean pollIsPaused = false;//拉取数据暂停
    private ReentrantLock lock = new ReentrantLock();
    private volatile boolean partitionsAssigning = false;//分区重新分配中，分区再平衡

    public interface ConsumerRecordCallback {
        void exec(ConsumerRecord<String, String> record) throws Exception;
    }

    public interface ConsumerRecordsCallback {
        void exec(ConsumerRecords<String, String> records) throws Exception;
    }


    /**
     * 将偏移量修改到最后提交的offset
     *
     * @param topicPartition
     */
    private void seekToCommitted(TopicPartition topicPartition) {
        try {
            lock.lock();
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);//当前组已提交的offset
            if (offsetAndMetadata != null) {
                //log.debug("seek 当前组的偏移量 {} {}", topicPartition, offsetAndMetadata);
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            } else {
                // 如果没有提交的偏移量，则可以选择从头开始或从末尾开始
                //log.debug("seek 当前组的偏移量 {} {}", topicPartition, 0);
                consumer.seek(topicPartition, 0); // 从头开始
                // 或者 consumer.seek(topicPartition, consumer.endOffsets(topicPartition) + 1); // 从末尾开始
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将偏移量修改到最后提交的offset
     */
    private void seekToCommitted() {
        try {
            lock.lock();
            for (TopicPartition topicPartition : consumer.assignment()) {
                seekToCommitted(topicPartition);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * seek偏移量到指定为止
     *
     * @param offsets
     */
    private void seekToOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            lock.lock();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata : offsets.entrySet()) {
                TopicPartition topicPartition = topicPartitionOffsetAndMetadata.getKey();
                OffsetAndMetadata offsetAndMetadata = topicPartitionOffsetAndMetadata.getValue();
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取消息
     *
     * @param pollTime 拉取消息时长
     * @return
     */
    private ConsumerRecords<String, String> getRecords(long pollTime) {
        try {
            lock.lock();
            partitionsAssigning = false;
            return consumer.poll(pollTime);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 暂停拉取消息
     */
    private void pause() {
        try {
            lock.lock();
            pollIsPaused = true;
            consumer.pause(consumer.assignment().toArray(new TopicPartition[0]));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复拉取消息
     */
    private void resume() {
        try {
            lock.lock();
            pollIsPaused = false;
            consumer.resume(consumer.assignment().toArray(new TopicPartition[0]));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 设置kafka地址
     *
     * @param bootstrapServers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaConsumerUtil setBootstrapServers(String bootstrapServers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }

    /**
     * 设置消费者组
     *
     * @param groupId 组id
     * @return
     */
    public KafkaConsumerUtil setGroupId(String groupId) {
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return this;
    }

    /**
     * 设置topic集合
     *
     * @param topics 主题集合
     * @return
     */
    public KafkaConsumerUtil setTopics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    /**
     * 设置topic
     *
     * @param topic 主题
     * @return
     */
    public KafkaConsumerUtil setTopic(String topic) {
        topics = Arrays.asList(topic);
        return this;
    }

    /**
     * 持续消费，一条条处理，如果回调方法抛出异常，则不会提交offset，出现异常那条消息会重新消费
     *
     * @param callback 回调处理一条消息
     */
    public void pollRecord(ConsumerRecordCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = getRecords(100);
            if (records != null && records.count() > 0) {
                pause();
                for (ConsumerRecord<String, String> record : records) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    try {
                        callback.exec(record);
                        try {
                            lock.lock();
                            if (partitionsAssigning) {//如果已经触发了重平衡，那么就不需要提交offsets了，可能会提交失败
                                log.info("分区已触发重平衡，需要重新拉取数据，本条数据未提交offset");
                                break;
                            }
                            offsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
                            consumer.commitSync(offsets);
                        } catch (Exception e) {
                            log.error("这条消息处理成功，但提交offset失败 {} {}", record, e.getMessage());
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        log.error("这条消息处理出现异常，回退offset到处理前的偏移量 {}", e.getMessage());
                        offsets.put(topicPartition, new OffsetAndMetadata(record.offset()));
                        seekToOffsets(offsets);
                        break;
                    }
                }
                resume();
            }
        }
    }

    /**
     * 持续消费，一批批处理，如果回调方法抛出异常，则不会提交offset，这批消息会重新消费
     *
     * @param pollTime 拉取消息等待时间(建议设置100毫秒)
     * @param callback 回调处理这一批消息
     */
    public void pollRecords(long pollTime, ConsumerRecordsCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = getRecords(pollTime);
            if (records != null && records.count() > 0) {
                Map<TopicPartition, OffsetAndMetadata> firstOffsets = new HashMap<>();
                Map<TopicPartition, OffsetAndMetadata> lastOffsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    if (!firstOffsets.containsKey(topicPartition)) {
                        firstOffsets.put(topicPartition, new OffsetAndMetadata(record.offset()));
                    }
                    lastOffsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
                }
                pause();
                try {
                    callback.exec(records);
                    try {
                        lock.lock();
                        if (partitionsAssigning) {//如果已经触发了重平衡，那么就不需要提交offsets了，可能会提交失败
                            log.info("分区已触发重平衡，需要重新拉取数据，本批数据未提交offsets");
                            continue;
                        }
                        consumer.commitSync(lastOffsets);
                    } catch (Exception e) {
                        log.error("这批消息处理成功，但提交offsets失败 {}", e.getMessage());
                    } finally {
                        lock.unlock();
                    }
                } catch (Exception e) {
                    log.error("这批消息处理出现异常，回退offsets到处理前的偏移量 {}", e.getMessage());
                    seekToOffsets(firstOffsets);
                }
                resume();
            }
        }
    }


    /**
     * 私有构造函数，防止外部实例化
     */
    private KafkaConsumerUtil() {
    }

    /**
     * 获取工具类工厂
     *
     * @return
     */
    public static KafkaConsumerUtil builder() {
        return new KafkaConsumerUtil();
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
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //在消费者重新平衡开始时调用，这个方法在分区被撤销之前调用。你可以在这里提交偏移量或者执行其他清理工作。
                try {
                    lock.lock();
                    if (CollUtil.isNotEmpty(partitions)) {
                        log.info("{} 触发分区重平衡，平衡前拥有 {} 个分区 {}", config.get(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                    } else {
                        log.info("{} 进行分区平衡", config.get(ConsumerConfig.GROUP_ID_CONFIG));
                    }
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //在消费者重新平衡完成后调用，这个方法在新分配的分区被分配给消费者之后调用。你可以在这里初始化资源或重置状态。
                try {
                    lock.lock();
                    partitionsAssigning = true;//标记分区重平衡，在commit offsets之前判断是否重新拉取数据
                    log.info("{} 分区平衡完毕，拿到了 {} 个分区 {}", config.get(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                    for (TopicPartition topicPartition : partitions) {
                        seekToCommitted(topicPartition);
                    }
                } finally {
                    lock.unlock();
                }
            }
        });

        //维持心跳，避免消息处理超时导致重平衡
        ThreadUtil.execute(() -> {
            while (keepConsuming) {
                ThreadUtil.sleep(1000 * 5);
                try {
                    lock.lock();
                    if (pollIsPaused && partitionsAssigning == false) {
                        consumer.poll(0);//当暂停拉取消息时，调用poll，只是发心跳，不会将消息拉取回来，不会改变offsets
                    }
                } finally {
                    lock.unlock();
                }
            }
        });

        return this;
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
            log.warn("关闭consumer出现异常 {}", e.getMessage());
        }
    }

}