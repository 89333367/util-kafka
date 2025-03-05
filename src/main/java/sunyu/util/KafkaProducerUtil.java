package sunyu.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka生产者工具类
 *
 * @author SunYu
 */
public class KafkaProducerUtil implements AutoCloseable {
    // 使用Hutool的日志工具记录日志
    private final Log log = LogFactory.get();
    // 配置信息
    private final Config config;

    /**
     * 获取一个新的构建器实例
     *
     * @return 构建器实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 私有构造函数，通过配置信息创建Kafka生产者实例
     *
     * @param config 配置信息
     */
    private KafkaProducerUtil(Config config) {
        log.info("[创建kafka生产者] 开始");
        // 配置客户端id
        //config.props.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());
        config.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 检查acks配置是否合法
        if (config.props.containsKey(ProducerConfig.ACKS_CONFIG)) {
            if (!config.props.get(ProducerConfig.ACKS_CONFIG).equals("0")
                    && !config.props.get(ProducerConfig.ACKS_CONFIG).equals("1")
                    && !config.props.get(ProducerConfig.ACKS_CONFIG).equals("-1")
                    && !config.props.get(ProducerConfig.ACKS_CONFIG).equals("all")) {
                // 若不合法，记录错误日志并抛出异常
                String err = StrUtil.format("[参数错误] acks参数不在参数范围内 传递值 {} 参数范围 [0,1,-1,all]", config.props.get(ProducerConfig.ACKS_CONFIG));
                log.error(err);
                throw new RuntimeException(err);
            }
        }
        // 创建Kafka生产者实例
        config.producer = new KafkaProducer<>(config.props);
        log.info("[创建kafka生产者] 结束");
        this.config = config;
    }

    /**
     * 私有静态内部类，用于存储配置信息
     */
    private static class Config {
        private Producer<String, String> producer;// Kafka生产者实例
        private final Properties props = new Properties();// Kafka配置属性
    }

    /**
     * 静态内部类，用于构建KafkaProducerUtil实例
     */
    public static class Builder {
        // 配置信息
        private final Config config = new Config();

        /**
         * 构建KafkaProducerUtil实例
         *
         * @return KafkaProducerUtil实例
         */
        public KafkaProducerUtil build() {
            return new KafkaProducerUtil(config);
        }

        /**
         * 设置kafka地址
         *
         * @param servers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
         * @return 构建器实例
         */
        public Builder bootstrapServers(String servers) {
            // 设置Kafka服务器地址
            config.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return this;
        }

        /**
         * 用于设置 Kafka 生产者发送消息确认的方式。参数 "all" 表示当所有的 follower 副本都收到消息后，才视为消息已提交。
         * 这种方式提供最高的数据完整性，但它可能会增加延迟。可以将参数指定为一个字符串类型的数字，以在保证一定程度可靠性的前提下减少延迟。
         * acks 参数有以下可选值：
         * "0": 生产者在成功写入消息后不会等待任何来自服务器的响应。快速但不可靠。
         * "1": 在主分区leader接收到消息后，生产者将得到来自服务器的成功响应。如果消息在follower副本中复制之前leader已经挂了，则消息将丢失。
         * "all"（或 "-1"）: 生产者会等待broker将消息写入所有副本后进行响应。此设置提供了最大的耐久性和数据完整性，但恢复时间较长。
         * "n": 等待 ISR 中至少 n 个 replicated 副本同步之后，向客户端发送确认。（n 不能为空、不能小于 1 或大于要求 topic 全部 replicationFactor）
         * acks 参数是一个极其重要的生产者配置参数。根据需要进行适当的调整，以便在可靠性和性能之间找到最佳折衷。
         *
         * @param acks 消息确认方式[0,1,-1,all]
         * @return 构建器实例
         */
        public Builder acks(String acks) {
            // 设置消息确认方式
            config.props.put(ProducerConfig.ACKS_CONFIG, acks);
            return this;
        }
    }

    /**
     * 实现AutoCloseable接口的方法，用于关闭Kafka生产者资源
     */
    @Override
    public void close() {
        log.info("[回收kafka生产者] 开始");

        log.info("[刷新缓存] 开始");
        flush();
        log.info("[刷新缓存] 结束");

        log.info("[关闭生产者] 开始");
        config.producer.close();
        log.info("[关闭生产者] 结束");

        log.info("[回收kafka生产者] 结束");
    }

    /**
     * 刷新缓存
     */
    public void flush() {
        config.producer.flush();
    }

    /**
     * 获得原生producer
     *
     * @return 原生producer
     */
    public Producer<String, String> getProducer() {
        return config.producer;
    }

    /**
     * 发送消息
     *
     * @param topic 主题
     * @param key   消息键
     * @param value 消息值
     * @return Future
     */
    public Future<RecordMetadata> send(String topic, String key, String value) {
        ProducerRecord<String, String> record;
        if (StrUtil.isBlank(key)) {
            record = new ProducerRecord<>(topic, value);
        } else {
            record = new ProducerRecord<>(topic, key, value);
        }
        return config.producer.send(record);
    }

}
