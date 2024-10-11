package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka消费者工具类
 *
 * @author 孙宇
 */
public class KafkaConsumerUtil implements Serializable, Closeable {
    private Log log = LogFactory.get();

    private AtomicBoolean run = new AtomicBoolean(true);//代表当前消费者是否在运行状态
    private Properties config = new Properties();//消费者参数配置
    private Consumer<String, String> consumer;//消费者对象
    private List<String> topics;//消费主题列表
    private ReentrantLock lock = new ReentrantLock();//可重入互斥锁
    private AtomicReference<Future> asyncTask = new AtomicReference<>(null);//异步执行对象


    /**
     * 设置kafka地址
     *
     * @param servers kafka地址，多个地址使用英文半角逗号分割(cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092)
     * @return
     */
    public KafkaConsumerUtil bootstrapServers(String servers) {
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return this;
    }

    /**
     * 设置消费者组
     *
     * @param id 组id
     * @return
     */
    public KafkaConsumerUtil groupId(String id) {
        config.put(ConsumerConfig.GROUP_ID_CONFIG, id);
        return this;
    }

    /**
     * 设置topic集合
     *
     * @param topics 主题集合
     * @return
     */
    public KafkaConsumerUtil topics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    /**
     * 设置topic
     *
     * @param topic 主题
     * @return
     */
    public KafkaConsumerUtil topic(String topic) {
        topics = Arrays.asList(topic);
        return this;
    }


    /**
     * 单条消息回调接口
     */
    public interface ConsumerRecordCallback {
        void exec(ConsumerRecord<String, String> record) throws Exception;
    }

    /**
     * 多条消息回调接口
     */
    public interface ConsumerRecordsCallback {
        void exec(ConsumerRecords<String, String> records) throws Exception;
    }


    /**
     * 取消异步调用，中断任务
     */
    private void cancelExec() {
        Future task = asyncTask.get();
        if (task != null) {
            task.cancel(true);//中断任务
            log.info("中断任务");
        }
    }


    /**
     * 重置读取偏移量
     */
    private void seekToOffset() {
        //log.debug("重置读取偏移量 开始");
        for (TopicPartition topicPartition : consumer.assignment()) {
            seekToOffset(topicPartition);
        }
        //log.debug("重置读取偏移量 结束");
    }

    /**
     * 重置读取偏移量
     *
     * @param topicPartition
     */
    private void seekToOffset(TopicPartition topicPartition) {
        OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);//当前组已提交的offset
        if (offsetAndMetadata != null) {
            //log.debug("重置当前组的偏移量 {} {}", topicPartition, offsetAndMetadata);
            consumer.seek(topicPartition, offsetAndMetadata.offset());
        } else {
            //log.debug("重置当前组的偏移量 {} {}", topicPartition, 0);
            consumer.seek(topicPartition, 0); // 从头开始
        }
    }


    /**
     * 持续消费，一条条处理，如果回调方法抛出异常，则不会提交offset，出现异常那条消息会重新消费
     *
     * @param callback 回调处理一条消息
     */
    public void pollRecord(ConsumerRecordCallback callback) {
        while (run.get()) {
            ConsumerRecords<String, String> records;
            try {
                lock.lock();
                seekToOffset();
                records = consumer.poll(100);
            } finally {
                lock.unlock();
            }
            //log.debug("拉取到 {} 条数据", records.count());
            if (records.isEmpty()) {
                ThreadUtil.sleep(100);
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                asyncTask.set(ThreadUtil.execAsync(() -> {
                    try {
                        callback.exec(record);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
                try {
                    asyncTask.get().get();
                    try {
                        lock.lock();
                        consumer.commitSync(new HashMap<TopicPartition, OffsetAndMetadata>() {{
                            put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                        }});
                    } finally {
                        lock.unlock();
                    }
                } catch (CancellationException e) {
                    log.warn("任务被中断");
                    break;
                } catch (Exception e) {
                    log.error("消息处理出现异常 {} {}", e.getClass(), e.getMessage());
                    break;
                } finally {
                    asyncTask.set(null);
                }
            }
        }
    }

    /**
     * 持续消费，一批批处理，如果回调方法抛出异常，则不会提交offset，这批消息会重新消费
     *
     * @param pollTime 拉取消息等待时间(建议设置100毫秒)，时间越长拉取数据会越多
     * @param callback 回调处理这一批消息
     */
    public void pollRecords(long pollTime, ConsumerRecordsCallback callback) {
        while (run.get()) {
            ConsumerRecords<String, String> records;
            try {
                lock.lock();
                seekToOffset();
                records = consumer.poll(pollTime);
            } finally {
                lock.unlock();
            }
            //log.debug("拉取到 {} 条数据", records.count());
            if (records.isEmpty()) {
                ThreadUtil.sleep(100);
                continue;
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }
            asyncTask.set(ThreadUtil.execAsync(() -> {
                try {
                    callback.exec(records);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            try {
                asyncTask.get().get();
                try {
                    lock.lock();
                    consumer.commitSync(offsets);
                } finally {
                    lock.unlock();
                }
            } catch (CancellationException e) {
                log.warn("任务被中断");
            } catch (Exception e) {
                log.error("批量消息处理出现异常 {} {}", e.getClass(), e.getMessage());
            } finally {
                asyncTask.set(null);
            }
        }
    }


    /**
     * 私有构造函数，防止外部实例化
     */
    private KafkaConsumerUtil() {
    }

    /**
     * 新建工具类工厂
     *
     * @return
     */
    public static KafkaConsumerUtil builder() {
        return new KafkaConsumerUtil();
    }


    /**
     * 构建工具类
     *
     * @param config ConsumerConfig参数
     * @return
     */
    public KafkaConsumerUtil build(Properties config) {
        log.info("构建消费者工具开始");
        if (consumer != null) {
            log.warn("消费者工具已构建，不要重复构建工具");
            return this;
        }

        config.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//禁用自动提交
        if (!config.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        }
        if (!config.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (!config.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        consumer = new KafkaConsumer<>(config);
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //在消费者重新平衡开始时调用，这个方法在分区被撤销之前调用。你可以在这里提交偏移量或者执行其他清理工作。
                log.debug("onPartitionsRevoked 开始");
                cancelExec();//取消异步调用
                lock.lock();
                if (CollUtil.isNotEmpty(partitions)) {
                    log.info("{} 触发分区重平衡，平衡前拥有 {} 个分区 {}", config.get(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                } else {
                    log.info("{} 进行分区平衡", config.get(ConsumerConfig.GROUP_ID_CONFIG));
                }
                log.debug("onPartitionsRevoked 结束");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //在消费者重新平衡完成后调用，这个方法在新分配的分区被分配给消费者之后调用。你可以在这里初始化资源或重置状态。
                log.debug("onPartitionsAssigned 开始");
                log.info("{} 分区平衡完毕，拿到了 {} 个分区 {}", config.get(ConsumerConfig.GROUP_ID_CONFIG), partitions.size(), partitions);
                lock.unlock();
                log.debug("onPartitionsAssigned 结束");
            }
        });

        //维持心跳，避免消息处理超时导致重平衡
        log.info("为消费者扩展心跳机制，维持心跳避免消息处理时间过长导致重平衡");
        ThreadUtil.execute(() -> {
            while (run.get()) {
                ThreadUtil.sleep(1000 * 5);
                try {
                    lock.lock();
                    consumer.pause(consumer.assignment().toArray(new TopicPartition[0]));
                    //log.debug("维持心跳");
                    ConsumerRecords<String, String> records = consumer.poll(0);
                    int count = records.count();
                    if (count > 0) {
                        log.warn("不应该走到这里，会丢失 {} 条数据", count);
                        Set<TopicPartition> topicPartitionSet = new HashSet<>();
                        for (ConsumerRecord<String, String> record : records) {
                            topicPartitionSet.add(new TopicPartition(record.topic(), record.partition()));
                        }
                        log.info("重置拉取偏移量，避免丢失数据 开始");
                        for (TopicPartition topicPartition : topicPartitionSet) {
                            seekToOffset(topicPartition);
                        }
                        log.info("重置拉取偏移量，避免丢失数据 完毕");
                    }
                    consumer.resume(consumer.assignment().toArray(new TopicPartition[0]));
                } finally {
                    lock.unlock();
                }
            }
        });

        log.info("构建消费者工具完毕");
        return this;
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
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//禁用自动提交
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return build(config);
    }


    /**
     * 停止消费，释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() {
        cancelExec();//取消异步调用
        run.set(false);
        log.info("销毁消费者工具开始");
        try {
            log.info("关闭消费者对象开始");
            try {
                lock.lock();
                consumer.close();
            } finally {
                lock.unlock();
            }
            log.info("关闭消费者对象成功");
        } catch (Exception e) {
            log.warn("关闭消费者对象失败 {}", e.getMessage());
        }
        config.clear();
        consumer = null;
        topics = null;
        log.info("销毁消费者工具完毕");
    }

}