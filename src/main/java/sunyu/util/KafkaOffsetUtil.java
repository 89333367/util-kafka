package sunyu.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

/**
 * kafka偏移量工具
 *
 * @author SunYu
 */
public class KafkaOffsetUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private KafkaOffsetUtil(Config config) {
        log.info("[构建{}] 开始", this.getClass().getSimpleName());
        if (!config.props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new RuntimeException("[参数错误] bootstrapServers不能为空");
        }
        if (!config.props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new RuntimeException("[参数错误] groupId不能为空");
        }
        if (CollUtil.isEmpty(config.topics)) {
            throw new RuntimeException("[参数错误] topic不能为空");
        }
        if (config.props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            String reset = config.props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            if (!reset.equals(OffsetResetStrategy.EARLIEST.name().toLowerCase())
                    && !reset.equals(OffsetResetStrategy.LATEST.name().toLowerCase())) {
                throw new RuntimeException("[参数错误] autoOffsetReset参数不在参数范围内 传递值 " + reset + " 参数范围 ["
                        + OffsetResetStrategy.EARLIEST.name().toLowerCase() + ","
                        + OffsetResetStrategy.LATEST.name().toLowerCase() + "]");
            }
        }
        //config.props.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//禁用自动提交
        config.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.consumer = new KafkaConsumer<>(config.props);
        for (String topic : config.topics) {
            List<PartitionInfo> partitionInfos = config.consumer.partitionsFor(topic);
            if (CollUtil.isNotEmpty(partitionInfos)) {
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
                    config.topicPartitions.add(topicPartition);
                }
            } else {
                log.warn("[无分区信息] topic：{}", topic);
            }
        }
        config.consumer.assign(config.topicPartitions);
        log.info("[构建{}] 结束", this.getClass().getSimpleName());
        this.config = config;
    }

    private static class Config {
        private Consumer<String, String> consumer;//消费者对象
        private final Properties props = new Properties();// Kafka配置属性
        private final List<String> topics = new ArrayList<>();//消费主题列表
        private final List<TopicPartition> topicPartitions = new ArrayList<>();//主题与分区信息
    }

    public static class Builder {
        private final Config config = new Config();

        public KafkaOffsetUtil build() {
            return new KafkaOffsetUtil(config);
        }

        /**
         * 配置kafka地址
         *
         * @param servers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
         * @return
         */
        public Builder bootstrapServers(String servers) {
            config.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return this;
        }

        /**
         * 配置消费组id
         *
         * @param groupId 消费组id
         * @return
         */
        public Builder groupId(String groupId) {
            config.props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return this;
        }

        /**
         * 加入一个主题
         *
         * @param topic 主题
         * @return
         */
        public Builder topic(String topic) {
            if (!config.topics.contains(topic)) {
                config.topics.add(topic);
            }
            return this;
        }

        /**
         * 加入多个主题
         *
         * @param topics
         * @return
         */
        public Builder topics(Collection<String> topics) {
            if (CollUtil.isNotEmpty(topics)) {
                for (String topic : topics) {
                    topic(topic);
                }
            }
            return this;
        }

        /**
         * 配置auto.offset.reset
         *
         * @param reset 取值范围：earliest、latest
         * @return
         */
        public Builder autoOffsetReset(String reset) {
            config.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁{}] 开始", this.getClass().getSimpleName());
        config.consumer.close();
        log.info("[销毁{}] 结束", this.getClass().getSimpleName());
    }

    /**
     * 获得主题与分区信息
     *
     * @return
     */
    public List<TopicPartition> getTopicPartitions() {
        return config.topicPartitions;
    }

    /**
     * 获得Earliest的偏移量
     *
     * @param topicPartition
     * @return 偏移量
     */
    public long getEarliestOffset(TopicPartition topicPartition) {
        config.consumer.seekToBeginning(topicPartition);
        return config.consumer.position(topicPartition);
    }

    /**
     * 获得Latest的偏移量
     *
     * @param topicPartition
     * @return 偏移量
     */
    public long getLatestOffset(TopicPartition topicPartition) {
        config.consumer.seekToEnd(topicPartition);
        return config.consumer.position(topicPartition);
    }

    /**
     * 获得当前的偏移量
     *
     * @param topicPartition
     * @return 偏移量
     */
    public long getCurrentOffset(TopicPartition topicPartition) {
        OffsetAndMetadata committed = config.consumer.committed(topicPartition);
        if (committed != null) {
            return committed.offset();
        } else {
            String reset = config.props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    OffsetResetStrategy.EARLIEST.name().toLowerCase());
            if (reset.equals(OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                config.consumer.seekToBeginning(topicPartition);
            } else {
                config.consumer.seekToEnd(topicPartition);
            }
            return config.consumer.position(topicPartition);
        }
    }

    /**
     * 获得当前用户的偏移量
     *
     * @return
     */
    public Map<TopicPartition, Long> getCurrentOffsets() {
        Map<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition topicPartition : getTopicPartitions()) {
            map.put(topicPartition, getCurrentOffset(topicPartition));
        }
        return map;
    }

    /**
     * 修复当前组的偏移量
     */
    public void fixCurrentOffsets() {
        log.info("[修复当前组的偏移量] 开始");
        String reset = config.props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.name().toLowerCase());
        for (TopicPartition topicPartition : config.topicPartitions) {
            long currentOffset = getCurrentOffset(topicPartition);
            if (reset.equals(OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                long earliestOffset = getEarliestOffset(topicPartition);
                if (currentOffset < earliestOffset) {
                    log.info("[修复偏移量] 主题:{} 分区:{} {} fix-> {}", topicPartition.topic(), topicPartition.partition(),
                            currentOffset, earliestOffset);
                    seekAndCommit(topicPartition, earliestOffset);
                }
            } else {
                long latestOffset = getLatestOffset(topicPartition);
                if (currentOffset > latestOffset) {
                    log.info("[修复偏移量] 主题:{} 分区:{} {} fix-> {}", topicPartition.topic(), topicPartition.partition(),
                            currentOffset, latestOffset);
                    seekAndCommit(topicPartition, latestOffset);
                }
            }
        }
        log.info("[修复当前组的偏移量] 结束");
    }

    /**
     * 改变当前组的偏移量
     *
     * @param topicPartition
     * @param offset
     */
    public void seekAndCommit(TopicPartition topicPartition, long offset) {
        log.debug("[改变偏移量] 组:{} 主题:{} 分区:{} 偏移量:{}", config.props.get(ConsumerConfig.GROUP_ID_CONFIG),
                topicPartition.topic(), topicPartition.partition(), offset);
        config.consumer.pause(topicPartition);
        config.consumer.seek(topicPartition, offset);
        config.consumer.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {
            {
                put(topicPartition, new OffsetAndMetadata(offset));
            }
        });
        config.consumer.resume(topicPartition);
    }

    /**
     * 改变当前组的偏移量
     *
     * @param offsets
     */
    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("[改变偏移量] 组:{} 偏移量:{}", config.props.get(ConsumerConfig.GROUP_ID_CONFIG), offsets);
        config.consumer.commitSync(offsets);
    }

    /**
     * 获得kafka配置参数
     *
     * @return Map
     */
    public Map<String, String> getKafkaParamsMap() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<?, ?> entry : config.props.entrySet()) {
            map.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return map;
    }

}
