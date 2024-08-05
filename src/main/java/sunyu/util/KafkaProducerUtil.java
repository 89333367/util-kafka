package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka生产者工具类
 *
 * @author 孙宇
 */
public class KafkaProducerUtil implements Serializable, Closeable {
    private Log log = LogFactory.get();


    private Properties config = new Properties();
    private Producer<String, String> producer;

    public interface ProducerCallback {
        void exec(RecordMetadata metadata, Exception exception) throws Exception;
    }

    /**
     * 设置kafka地址
     *
     * @param bootstrapServers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaProducerUtil setBootstrapServers(String bootstrapServers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }


    /**
     * 异步发送一条消息，需要自己处理Future
     *
     * @param topic 主题
     * @param key   键
     * @param value 值
     * @return Future
     */
    public Future<RecordMetadata> sendAsync(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

    /**
     * 异步发送一条消息，需要自己处理Future
     *
     * @param topic    主题
     * @param key      键
     * @param value    值
     * @param callback complete回调
     * @return Future
     */
    public Future<RecordMetadata> sendAsync(String topic, String key, String value, ProducerCallback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record, (metadata, exception) -> {
            try {
                callback.exec(metadata, exception);
            } catch (Exception e) {
                log.error(e);
            }
        });
    }

    /**
     * 同步发送一条消息
     *
     * @param topic 主题
     * @param key   键
     * @param value 值
     */
    public void sendSync(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            log.error(e);
        } catch (ExecutionException e) {
            log.error(e);
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * 同步发送一条消息
     *
     * @param topic    主题
     * @param key      键
     * @param value    值
     * @param callback complete回调
     */
    public void sendSync(String topic, String key, String value, ProducerCallback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record, (metadata, exception) -> {
                try {
                    callback.exec(metadata, exception);
                } catch (Exception e) {
                    log.error(e);
                }
            }).get();
        } catch (InterruptedException e) {
            log.error(e);
        } catch (ExecutionException e) {
            log.error(e);
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * 将缓存中累计的消息发送出去
     */
    public void flush() {
        producer.flush();
    }


    /**
     * 私有构造函数，防止外部实例化
     */
    private KafkaProducerUtil() {
    }

    /**
     * 获取工具类工厂
     *
     * @return
     */
    public static KafkaProducerUtil builder() {
        return new KafkaProducerUtil();
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public KafkaProducerUtil build() {
        if (producer != null) {
            return this;
        }
        //producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 用于设置 Kafka 生产者发送消息确认的方式。参数 "all" 表示当所有的 follower 副本都收到消息后，才视为消息已提交。
         * 这种方式提供最高的数据完整性，但它可能会增加延迟。可以将参数指定为一个字符串类型的数字，以在保证一定程度可靠性的前提下减少延迟。
         * acks 参数有以下可选值：
         * "0": 生产者在成功写入消息后不会等待任何来自服务器的响应。快速但不可靠。
         * "1": 在主分区leader接收到消息后，生产者将得到来自服务器的成功响应。如果消息在follower副本中复制之前leader已经挂了，则消息将丢失。
         * "all"（或 "-1"）: 生产者会等待broker将消息写入所有副本后进行响应。此设置提供了最大的耐久性和数据完整性，但恢复时间较长。
         * "n": 等待 ISR 中至少 n 个 replicated 副本同步之后，向客户端发送确认。（n 不能为空、不能小于 1 或大于要求 topic 全部 replicationFactor）
         * acks 参数是一个极其重要的生产者配置参数。根据需要进行适当的调整，以便在可靠性和性能之间找到最佳折衷。
         */
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        /**
         * 用于设置当 Kafka 生产者发送消息失败后的重试次数。在这个配置中，将重试次数设置为 0 表示当出现错误时不会尝试重发消息。
         * 如果生产者无法成功地发送消息，则该消息将被视为发送失败并且不会进行任何重试。这意味着发生错误后，生产者将立即将该消息标记为发送失败，并抛出一个异常。
         * 默认情况下，retries 配置被设置为 2147483647，即尝试无限次数重试直到发送成功。通常情况下，建议仅在服务器存在故障的情况下才启用重试功能。
         * 最好根据您应用程序特定的可靠性需求来选择重试数量和间隔时间。通过设置正确的"acks" 和 "retry" 策略，可以实现高效且可靠的 Kafka 消息传递。
         */
        config.put(ProducerConfig.RETRIES_CONFIG, 0);

        /**
         * 用于设置生产者在发送数据之前等待收集到的消息批量大小。具体来说，这个参数指定了一批消息的大小，当达到这个大小后，
         * 生产者将一次性将多条消息打包成一个批次进行发送，以减少网络传输和 Kafka 服务端处理开销。默认值为 16KB。
         * 根据实际业务场景和网络状况，可以根据需要自定义此参数。增大批次大小可以提高吞吐量和效率，但同时也会增加延迟；
         * 缩小批次大小可以降低延迟，但会导致更多的网络请求和处理连接开销。
         * 一般建议根据具体应用情况调整参数值，以提高生产者的效率和吞吐量，并保持合理的网络负荷和延迟水平。
         */
        //producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        /**
         * 用于控制在将 messages 发送到 broker 前消息可等待的时间。具体来说，
         * 这个参数指定了生产者在将未满足 batch.size 和 compression.type 要求下的所有消息发送到 Kafka broker 之前，
         * 向下一次尝试发送数据之间的最大延迟时间。此参数的默认值为 0，即立即发送所有缓存中的消息。
         * 举例来说，如果设置了 linger.ms=1，则生产者将会等待 1 毫秒以允许更多的消息添加到 batch 中，
         * 以组成更大的消息集合，从而减少网络 IO 请求的数量，提高整体吞吐率。但是，
         * 设置较高的 linger 时间可能会导致较大的延迟，因为此时生产者可能要等待一段时间后才能发送该消息。
         * 一般建议根据实际业务场景和应用需求来自定义 linger.ms 的值。通常情况下，可以将其设置为 5ms - 100ms 之间的数值，以取得比较均衡的效果。
         */
        config.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        /**
         * 用于指定生产者可用于缓存发送消息的最大内存大小（单位为字节）。具体来说，这个参数指定了 Kafka 生产者应该保留多少内存空间来缓冲还未传送给 broker 的尚不完整的 batch。
         * 当生产者发送消息的速度比 broker 接收并处理消息的速度快时，这个 buffer.memory 配置就非常重要了。
         * 如果 Kafka 生产者的发送速度过于快，broker 可能无法及时处理所有收到的消息，导致消息积压。而如果 Kafka 生产者缓冲区大小设置过小，
         * 则可能会造成批量发送频次较高、带宽利用率低下等问题。因此，在调整这个参数时，需要结合使用场景和硬件资源情况进行合理设置。
         * 一般建议根据实际业务场景、消息大小和网络状况等因素来自定义 buffer.memory 的值。通常情况下，可以将其设置为物理内存总容量的 10% 至 25% 左右，
         * 以充分利用可用空间同时避免内存耗尽和系统抖动等问题。请注意，此值必须大于或等于 batch.size 和 compression.type 相关配置的总字节数。
         */
        //producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producer = new KafkaProducer<>(config);
        return this;
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() {
        try {
            producer.flush();
        } catch (Exception e) {
            log.error(e);
        }
        try {
            producer.close();
        } catch (Exception e) {
            log.error(e);
        }
    }


}