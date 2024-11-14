package sunyu.util;

import cn.hutool.core.util.IdUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
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
public class KafkaOffsetUtil implements Serializable, Closeable {
    private final Log log = LogFactory.get();

    private final Properties config = new Properties();
    private Consumer<String, String> consumer;//消费者对象
    private List<String> topics;//消费主题列表
    private final List<TopicPartition> topicPartitions = new ArrayList<>();

    /**
     * 设置kafka地址
     *
     * @param servers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaOffsetUtil bootstrapServers(String servers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return this;
    }

    /**
     * 设置消费者组
     *
     * @param id 组id
     * @return
     */
    public KafkaOffsetUtil groupId(String id) {
        config.put(ConsumerConfig.GROUP_ID_CONFIG, id);
        return this;
    }

    /**
     * 设置topic集合
     *
     * @param topics 主题集合
     * @return
     */
    public KafkaOffsetUtil topics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    /**
     * 设置topic
     *
     * @param topic 主题
     * @return
     */
    public KafkaOffsetUtil topic(String topic) {
        topics = Collections.singletonList(topic);
        return this;
    }

    /**
     * 获得主题与分区信息
     *
     * @return
     */
    public List<TopicPartition> getTopicPartitions() {
        return topicPartitions;
    }

    /**
     * 获取主题的分区信息
     *
     * @param topic
     * @return
     */
    public List<TopicPartition> getPartitions(String topic) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            if (topicPartition.topic().equals(topic)) {
                partitions.add(topicPartition);
            }
        }
        return partitions;
    }


    /**
     * 调整偏移量
     *
     * @param topic     主题
     * @param partition 分区号
     * @param offset    偏移量
     */
    public void seek(String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.seek(topicPartition, offset);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
    }

    /**
     * 调整偏移量到LATEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToEnd(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.seekToEnd(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = consumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
    }

    /**
     * 调整偏移量到EARLIEST
     *
     * @param topic     主题
     * @param partition 分区号
     */
    public void seekToBeginning(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.seekToBeginning(topicPartition);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        long offset = consumer.position(topicPartition);
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
    }

    /**
     * 获得最初的offset
     *
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> offsetEarliest() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            consumer.seekToBeginning(topicPartition);
            long offset = consumer.position(topicPartition);
            //log.info("EARLIEST offset {}-{} {}", topic, topicPartition.partition(), offset);
            offsets.put(topicPartition, new OffsetAndMetadata(offset));
        }
        return offsets;
    }

    /**
     * 获得最后的offset
     *
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> offsetLatest() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            consumer.seekToEnd(topicPartition);
            long offset = consumer.position(topicPartition);
            //log.info("LATEST offset {}-{} {}", topic, topicPartition.partition(), offset);
            offsets.put(topicPartition, new OffsetAndMetadata(offset));
        }
        return offsets;
    }

    /**
     * 获得当前的offset
     *
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> offsetCurrent() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            OffsetAndMetadata committed = consumer.committed(topicPartition);
            if (committed != null) {
                //log.info("CURRENT group offset {}-{} {}", topic, topicPartition.partition(), committed.offset());
                offsets.put(topicPartition, new OffsetAndMetadata(committed.offset()));
            } else {
                if (config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).equalsIgnoreCase(OffsetResetStrategy.LATEST.name())) {
                    consumer.seekToEnd(topicPartition);
                    long offset = consumer.position(topicPartition);
                    //log.info("LATEST offset {}-{} {}", topic, topicPartition.partition(), offset);
                    offsets.put(topicPartition, new OffsetAndMetadata(offset));
                } else {
                    consumer.seekToBeginning(topicPartition);
                    long offset = consumer.position(topicPartition);
                    //log.info("EARLIEST offset {}-{} {}", topic, topicPartition.partition(), offset);
                    offsets.put(topicPartition, new OffsetAndMetadata(offset));
                }
            }
        }
        // todo 修正offset，避免超出range
        Map<TopicPartition, OffsetAndMetadata> earliest = offsetEarliest();
        earliest.forEach((topicPartition, offsetAndMetadata) -> {
            OffsetAndMetadata cur = offsets.get(topicPartition);
            if (cur == null) {
                offsets.put(topicPartition, offsetAndMetadata);
            } else if (cur.offset() < offsetAndMetadata.offset()) {
                log.warn("当前 {}-{} 小于偏移量范围 {} < {} 进行修正", topicPartition.topic(), topicPartition.partition(), cur.offset(), offsetAndMetadata.offset());
                offsets.put(topicPartition, offsetAndMetadata);
            }
        });
        Map<TopicPartition, OffsetAndMetadata> latest = offsetLatest();
        latest.forEach((topicPartition, offsetAndMetadata) -> {
            OffsetAndMetadata cur = offsets.get(topicPartition);
            if (cur == null) {
                offsets.put(topicPartition, offsetAndMetadata);
            } else if (cur.offset() > offsetAndMetadata.offset()) {
                log.warn("当前 {}-{} 大于偏移量范围 {} > {} 进行修正", topicPartition.topic(), topicPartition.partition(), cur.offset(), offsetAndMetadata.offset());
                offsets.put(topicPartition, offsetAndMetadata);
            }
        });
        return offsets;
    }

    /**
     * 私有构造函数，防止外部实例化
     */
    private KafkaOffsetUtil() {
    }

    /**
     * 新建工具类工厂
     *
     * @return
     */
    public static KafkaOffsetUtil builder() {
        return new KafkaOffsetUtil();
    }


    public KafkaOffsetUtil build(Properties config) {
        log.info("构建偏移量工具开始");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        if (!config.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        }
        if (!config.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (!config.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        this.config.putAll(config);
        consumer = new KafkaConsumer<>(config);
        for (String topic : topics) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                topicPartitions.add(topicPartition);
            }
        }
        consumer.assign(topicPartitions);
        log.info("构建偏移量工具完毕");
        return this;
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
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return build(config);
    }

    @Override
    public void close() {
        consumer.close();
        log.info("销毁偏移量工具完毕");
    }


}