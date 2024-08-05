package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaProducerUtil;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestProducer {
    Log log = LogFactory.get();

    @Test
    void 同步发送消息() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值");
        kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
    }

    @Test
    void 同步发送消息并且自己处理metadata和exception() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值", (metadata, exception) -> {
            // 这里你可以自己处理 metadata 和 exception 异常信息
        });
        kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
    }

    @Test
    void 异步发送消息() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        Future<RecordMetadata> recordMetadataFuture = kafkaProducerUtil.sendAsync("主题", "键，这里可以为null", "值");
        try {
            recordMetadataFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
    }

    @Test
    void 异步发送消息并且自己处理metadata和exception() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        Future<RecordMetadata> recordMetadataFuture = kafkaProducerUtil.sendAsync("主题", "键，这里可以为null", "值", (metadata, exception) -> {
            // 这里你可以自己处理 metadata 和 exception 异常信息
        });
        try {
            recordMetadataFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
    }

    @Test
    void 批量发送消息() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        for (int i = 0; i < 10000; i++) {
            //这里发送消息
        }
        kafkaProducerUtil.flush();//批量发送后，调用一次flush即可，不需要在每一次循环中调用，减少网络交互
    }

    @Test
    void 关闭整个项目() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();
        //这里发送消息

        //如果整个项目需要关闭，调用close释放资源
        kafkaProducerUtil.close();
    }

    @Test
    void 构建传参() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .build(config);

        kafkaProducerUtil.sendSync("test_topic", "key1", "value1");

        //如果整个项目需要关闭，调用close释放资源
        kafkaProducerUtil.close();
    }
}