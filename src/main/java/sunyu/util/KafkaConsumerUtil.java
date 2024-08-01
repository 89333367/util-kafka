package sunyu.util;

import cn.hutool.core.map.MapUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
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
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //在消费者重新平衡完成后调用，这个方法在新分配的分区被分配给消费者之后调用。你可以在这里初始化资源或重置状态。
                if (MapUtil.isNotEmpty(offsets)) {//如果offsets不为空，则有需要提交的offset
                    log.debug("重平衡后提交未成功提交的offsets");
                    try {
                        consumer.poll(0);//先拉取一下才能commit
                        for (Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata : offsets.entrySet()) {
                            consumer.seek(topicPartitionOffsetAndMetadata.getKey(), topicPartitionOffsetAndMetadata.getValue().offset());
                        }
                        consumer.commitSync(offsets);
                    } catch (Exception e) {
                        log.error("重平衡后提交offsets出现异常 {}", e.getMessage());
                    }
                    offsets.clear();
                }
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
            log.warn("关闭consumer出现异常 {}", e);
        }
    }

    private boolean keepConsuming = true;
    private Properties config = new Properties();
    private Consumer<String, String> consumer;
    private List<String> topics;
    private volatile Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public interface ConsumerRecordCallback {
        void exec(ConsumerRecord<String, String> record) throws Exception;
    }

    public interface ConsumerRecordsCallback {
        void exec(ConsumerRecords<String, String> records) throws Exception;
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
    public KafkaConsumerUtil setTopics(String topic) {
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
                try {
                    callback.exec(record);//回调，由调用方处理消息，这里如果处理时间过长，下面提交offset可能会失败
                    try {
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));//记录消息偏移量+1
                        consumer.commitSync(offsets);
                        offsets.clear();
                    } catch (Exception e) {
                        log.warn("提交offset失败，但消息处理成功 {}", e.getMessage());
                        break;
                    }
                } catch (Exception e) {
                    log.error("此条消息处理出现异常 {} {}", record, e.getMessage());
                    // 如果消息处理异常，使用seek方法回退到当前消息，重新处理，避免丢失数据
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    break;//如果当前条消息处理异常了，后面就不要再处理了，跳出循环
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
            // 记录当前偏移量
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : consumer.assignment()) {
                offsets.put(partition, consumer.position(partition));
            }
            ConsumerRecords<String, String> records = consumer.poll(pollTime);
            try {
                callback.exec(records);
                consumer.commitAsync();
            } catch (Exception e) {
                log.error("这批消息处理出现异常 {}", e.getMessage());
                // seek回到poll之前的offset，避免消息丢失
                for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                    consumer.seek(entry.getKey(), entry.getValue());
                }
            }
        }
    }

}