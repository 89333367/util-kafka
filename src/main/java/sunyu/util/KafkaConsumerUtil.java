package sunyu.util;

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
        consumer.subscribe(topics);
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
     * 持续消费，一条条处理，如果不抛异常，则会自动提交offset
     *
     * @param callback 回调处理一条消息
     */
    public void pollRecord(ConsumerRecordCallback callback) {
        while (keepConsuming) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    callback.exec(record);//回调，由调用方处理消息
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));//记录消息偏移量+1
                    try {
                        consumer.commitSync(offsets);//提交偏移量
                    } catch (Exception e) {
                        log.warn("提交offsets出现异常 {}", offsets);
                        break;//提交offset失败，跳出循环，重新获取消息
                    }
                } catch (Exception e) {
                    log.error("处理消息出现异常 {} {}", record, e);
                    // 如果消息处理异常，使用seek方法回退到当前消息，重新处理
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    break;//如果当前条消息处理异常了，后面就不要再处理了，跳出循环
                }
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
                    log.warn("提交offsets出现异常");
                }
            } catch (Exception e) {
                log.error("这批消息处理失败 {}", e);
            }
        }
    }


}