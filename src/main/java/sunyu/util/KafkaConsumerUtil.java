package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka消费者工具
 *
 * @author SunYu
 */
public class KafkaConsumerUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private KafkaConsumerUtil(Config config) {
        log.info("[创建kafka消费者工具] 开始");
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
            if (!reset.equals(OffsetResetStrategy.EARLIEST.name().toLowerCase()) && !reset.equals(OffsetResetStrategy.LATEST.name().toLowerCase())) {
                throw new RuntimeException("[参数错误] autoOffsetReset参数不在参数范围内 传递值 " + reset + " 参数范围 [" + OffsetResetStrategy.EARLIEST.name().toLowerCase() + "," + OffsetResetStrategy.LATEST.name().toLowerCase() + "]");
            }
        }
        //config.props.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//禁用自动提交
        config.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.consumer = new KafkaConsumer<>(config.props);
        config.consumer.subscribe(config.topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                /**
                 * 触发时机：
                 * 当消费者组中的某个消费者实例失去分区的所有权时，onPartitionsRevoked 方法会被调用。这通常发生在以下情况：
                 * 消费者实例被关闭或崩溃。
                 * 消费者实例被移出消费者组（例如，消费者组的成员数量发生变化）。
                 * 消费者组的分区分配发生变化（例如，新的分区被添加到主题中）。
                 * 触发顺序：
                 * onPartitionsRevoked 方法会在再均衡开始之前和消费者停止读取消息之后被调用。
                 * 在这个方法中，消费者可以提交当前的偏移量，确保下一个接管分区的消费者知道从哪里开始读取消息。
                 */
                log.info("[消费者重新平衡] 开始");
                if (CollUtil.isNotEmpty(partitions)) {
                    log.info("{} 触发分区重平衡，平衡前拥有 {} 个分区 {}", config.props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                } else {
                    log.info("{} 进行分区平衡", config.props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
                }
                log.info("[消费者重新平衡] 结束");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                /**
                 * 触发时机：
                 * 当消费者组中的分区重新分配给消费者实例时，onPartitionsAssigned 方法会被调用。这通常发生在以下情况：
                 * 消费者组的成员数量发生变化（例如，新的消费者实例加入或现有消费者实例离开）。
                 * 消费者组的分区分配发生变化（例如，新的分区被添加到主题中）。
                 * 触发顺序：
                 * onPartitionsAssigned 方法会在重新分配分区之后和消费者开始读取消息之前被调用。
                 * 在这个方法中，消费者可以初始化资源或重置状态，准备处理新的分区
                 */
                log.info("[重新分配分区] 开始");
                log.info("{} 重新分配分区，拿到了 {} 个分区 {}", config.props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                log.info("[重新分配分区] 结束");
            }
        });
        log.info("[创建kafka消费者工具] 结束");
        this.config = config;
        //增加心跳功能
        ThreadUtil.execute(this::heartbeat);
    }

    private static class Config {
        private Consumer<String, String> consumer;//消费者对象
        private final Properties props = new Properties();// Kafka配置属性
        private final List<String> topics = new ArrayList<>();//消费主题列表
        private final AtomicBoolean heartbeat = new AtomicBoolean(false);//心跳状态
        private final ReentrantLock lock = new ReentrantLock();//可重入互斥锁
    }

    public static class Builder {
        private final Config config = new Config();

        public KafkaConsumerUtil build() {
            return new KafkaConsumerUtil(config);
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
        log.info("[回收kafka消费者工具] 开始");
        config.heartbeat.set(false);//结束心跳
        config.consumer.close();
        log.info("[回收kafka消费者工具] 结束");
    }

    /**
     * 模拟心跳，避免kafka超过30秒没有poll会触发再平衡
     */
    private void heartbeat() {
        while (true) {
            log.debug("[模拟kafka心跳]");
            if (config.heartbeat.get()) {
                try {
                    config.lock.lock();
                    TopicPartition[] partitions = config.consumer.assignment().toArray(new TopicPartition[0]);
                    config.consumer.pause(partitions);
                    //log.debug("维持心跳");
                    ConsumerRecords<String, String> records = config.consumer.poll(0);
                    if (!records.isEmpty()) {
                        log.info("[重置拉取偏移量] 避免丢失数据 开始");
                        Map<TopicPartition, Long> firstOffsets = new HashMap<>();
                        for (ConsumerRecord<String, String> record : records) {
                            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                            if (!firstOffsets.containsKey(topicPartition)) {
                                firstOffsets.put(topicPartition, record.offset());
                            }
                        }
                        firstOffsets.forEach((topicPartition, offset) -> {
                            // seek offset 回退，下次抓取还从这里抓取
                            config.consumer.seek(topicPartition, offset);
                        });
                        log.info("[重置拉取偏移量] 避免丢失数据 结束");
                    }
                    config.consumer.resume(partitions);
                } finally {
                    config.lock.unlock();
                }
            }
            ThreadUtil.sleep(1000 * 5);
        }
    }

    /**
     * 批量拉取数据
     *
     * @param pollTime       拉取时间，单位毫秒
     * @param recordsHandler 拉取到的消息处理函数
     */
    public void pollRecords(Long pollTime, java.util.function.Consumer<ConsumerRecords<String, String>> recordsHandler) {
        while (true) {
            Map<TopicPartition, Long> firstOffsets = new HashMap<>();
            try {
                if (pollTime == null) {
                    pollTime = 50L;
                }
                ConsumerRecords<String, String> records = config.consumer.poll(pollTime);
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    if (!firstOffsets.containsKey(topicPartition)) {
                        firstOffsets.put(topicPartition, record.offset());
                    }
                }
                log.debug("[firstOffsets] {}", firstOffsets);
                config.heartbeat.set(true);//开启心跳
                recordsHandler.accept(records); // 调用回调函数处理消息
                config.heartbeat.set(false);//结束心跳
                Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    commitOffsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
                }
                log.debug("[commitOffsets] {}", commitOffsets);
                try {
                    config.lock.lock();
                    config.consumer.commitSync(commitOffsets);
                } finally {
                    config.lock.unlock();
                }
            } catch (Exception e) {
                config.heartbeat.set(false);//结束心跳
                log.error("[批量消息处理失败] {}", ExceptionUtil.stacktraceToString(e));
                firstOffsets.forEach((topicPartition, offset) -> {
                    // seek offset 回退，下次抓取还从这里抓取
                    config.consumer.seek(topicPartition, offset);
                });
            } finally {
                config.heartbeat.set(false);//结束心跳
            }
        }
    }

}
