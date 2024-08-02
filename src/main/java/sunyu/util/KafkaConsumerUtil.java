package sunyu.util;

import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka消费者工具类
 *
 * @author 孙宇
 */
public enum KafkaConsumerUtil implements Serializable, Closeable {
    INSTANCE;
    private Log log = LogFactory.get();

    /**
     * 获取工具类工厂
     *
     * @return
     */
    public static KafkaConsumerUtil builder() {
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
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //在消费者重新平衡开始时调用，这个方法在分区被撤销之前调用。你可以在这里提交偏移量或者执行其他清理工作。
                log.info("{} 触发重平衡", config.get(ConsumerConfig.GROUP_ID_CONFIG));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //在消费者重新平衡完成后调用，这个方法在新分配的分区被分配给消费者之后调用。你可以在这里初始化资源或重置状态。
                log.info("{} 重平衡完毕，拿到了 {} 个分区 {}", config.get(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                try {
                    lock.lock();
                    for (TopicPartition topicPartition : partitions) {
                        seekToCommitted(topicPartition);
                    }
                } finally {
                    lock.unlock();
                }
            }
        });


        if (!CronUtil.getScheduler().isStarted()) {
            CronUtil.setMatchSecond(true);
            CronUtil.start();
        }
        CronUtil.schedule("kafkaConsumerSubmitSchedule", "0/1 * * * * ? ", (Task) () -> {
            try {
                lock.lock();
                consumer.commitAsync(waitCommitOffsets, (offsets, exception) -> {
                    if (exception != null) {
                        if (exception instanceof ConcurrentModificationException) {
                            log.warn("忽略ConcurrentModificationException异常");
                            commitOffsetsError = false;
                        } else {
                            commitOffsetsError = true;
                            waitCommitOffsets.clear();
                        }
                    }
                });
            } finally {
                lock.unlock();
            }
        });

        return INSTANCE;
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
        try {
            if (CronUtil.getScheduler().isStarted()) {
                CronUtil.stop();
            }
        } catch (Exception e) {
            log.warn("关闭定时任务失败 {}", e.getMessage());
        }
    }

    private boolean keepConsuming = true;
    private Properties config = new Properties();
    private Consumer<String, String> consumer;
    private List<String> topics;
    private volatile Map<TopicPartition, OffsetAndMetadata> waitCommitOffsets = new ConcurrentHashMap<>();
    private volatile boolean commitOffsetsError = false;
    private ReentrantLock lock = new ReentrantLock();

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
    public KafkaConsumerUtil setTopic(String topic) {
        topics = Arrays.asList(topic);
        return INSTANCE;
    }


    /**
     * 持续消费，一条条处理，如果回调方法抛出异常，则不会提交offset，出现异常那条消息会重新消费
     *
     * @param callback 回调处理一条消息
     */
    public void pollRecord(ConsumerRecordCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                try {
                    lock.lock();
                    if (commitOffsetsError) {//如果提交偏移量出错了，有可能是触发了再平衡，那么先seek到最后提交的offset，跳出循环，重新poll
                        commitOffsetsError = false;
                        seekToCommitted();
                        break;
                    }
                    //如果提交偏移量没出错，将当前record的偏移量记住
                    waitCommitOffsets.clear();
                    waitCommitOffsets.put(topicPartition, new OffsetAndMetadata(record.offset()));//先记录当前record偏移量，使用定时任务不断地提交，避免超时
                } finally {
                    lock.unlock();
                }
                try {
                    callback.exec(record);//回调，由调用方处理消息
                    try {
                        lock.lock();
                        if (commitOffsetsError) {//如果提交偏移量出错了，有可能是触发了再平衡，那么先seek到最后提交的offset，跳出循环，重新poll
                            commitOffsetsError = false;
                            seekToCommitted();
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                } catch (Exception e) {
                    log.error("此条消息处理出现异常 {} {}", record, e.getMessage());
                    try {
                        lock.lock();
                        seekToCommitted();//seek，后面会跳出循环，然后重新poll
                        waitCommitOffsets.clear();
                    } finally {
                        lock.unlock();
                    }
                    break;
                }
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
            ConsumerRecords<String, String> records = consumer.poll(pollTime);
            if (records.count() > 0) {
                try {
                    lock.lock();
                    if (commitOffsetsError) {//如果提交偏移量出错了，有可能是触发了再平衡，那么先seek到最后提交的offset，跳出循环，重新poll
                        commitOffsetsError = false;
                        seekToCommitted();
                        continue;
                    }
                    //如果提交偏移量没出错，将当前records的偏移量记住
                    waitCommitOffsets.clear();
                    for (ConsumerRecord<String, String> record : records) {
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        if (!waitCommitOffsets.containsKey(topicPartition)) {//只记录第一个
                            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
                            waitCommitOffsets.put(topicPartition, offsetAndMetadata);
                        }
                    }
                } finally {
                    lock.unlock();
                }
                try {
                    callback.exec(records);
                } catch (Exception e) {
                    log.error("这批消息处理出现异常 {}", e.getMessage());
                    try {
                        lock.lock();
                        seekToCommitted();//seek，下次循环重新poll
                        waitCommitOffsets.clear();
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }
    }

}