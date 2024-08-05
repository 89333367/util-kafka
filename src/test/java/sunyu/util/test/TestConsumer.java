package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaConsumerUtil;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TestConsumer {
    Log log = LogFactory.get();

    @Test
    void t001() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //持续消费，一条一条处理，处理完毕后，只要不抛异常，会自动提交offset
        kafkaConsumerUtil.pollRecord((record) -> {
            log.debug("收到消息 {}", record);
            //record.offset();
            //record.topic();
            //record.partition();
            //record.key();
            //record.value();
            ThreadUtil.sleep(5000);
            log.debug("处理完毕 {}", record);
        });
    }

    @Test
    void t002() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //持续消费，一批一批处理，处理完毕后，只要不抛异常，会自动提交offset
        kafkaConsumerUtil.pollRecords(100, records -> {
            log.debug("本批拉取了 {} 条消息", records);
            for (ConsumerRecord<String, String> record : records) {
                log.debug("{}", record);
                ThreadUtil.sleep(5000);//模拟record处理时间
            }
        });
    }


    @Test
    void t004() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .setTopic("US_GENERAL")
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_20240625")
                .build();
        kafkaConsumerUtil.pollRecord(consumerRecord -> {
            log.info("开始处理 {}", consumerRecord);
            ThreadUtil.sleep(1000 * 1);
            log.info("处理完毕 {}", consumerRecord);
        });
    }


    @Test
    void t005() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .setTopic("GENERAL_MSG")
                .setBootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        kafkaConsumerUtil.pollRecord(consumerRecord -> {
            log.info("开始处理 {}", consumerRecord);
            ThreadUtil.sleep(1000 * 5);
            //log.info("处理完毕 {}", consumerRecord);
        });
    }

    @Test
    void t006() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .setTopic("GENERAL_MSG")
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        kafkaConsumerUtil.pollRecords(100, consumerRecords -> {
            log.info("拉取了 {} 条", consumerRecords.count());
            log.info("第一个offsets {}", consumerRecords.iterator().next());
            ConsumerRecord<String, String> last = null;
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                //log.info("{}", consumerRecord);
                last = consumerRecord;
            }
            log.info("最后一个offsets {}", last);
            ThreadUtil.sleep(1000 * 10);
        });
    }


    @Test
    void t007() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                //.setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .setTopic("GENERAL_MSG")
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();//全局只要定义一个即可
        kafkaConsumerUtil.pollRecord(consumerRecord -> {
            log.info("开始处理 {}", consumerRecord);
            //处理消息，如果这里没有抛异常，则消息会自动提交offset，如果这里 throw Exception，那么这条消息不会提交offset，下次还会拉取回来
            //record.offset();
            //record.topic();
            //record.partition();
            //record.key();
            //record.value();
            ThreadUtil.sleep(1000 * 3);
            log.info("处理完毕 {}", consumerRecord);
        });
    }

    @Test
    void t008() {
        AtomicInteger i = new AtomicInteger();
        Properties config = new Properties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_kafka_consumer_util");
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                //.setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .setTopic("GENERAL_MSG")
                .build(config);//全局只要定义一个即可
        kafkaConsumerUtil.pollRecord(consumerRecord -> {
            if (i.get() >= 5) {
                kafkaConsumerUtil.close();
                return;
            }
            log.info("开始处理 {}", consumerRecord);
            //处理消息，如果这里没有抛异常，则消息会自动提交offset，如果这里 throw Exception，那么这条消息不会提交offset，下次还会拉取回来
            //record.offset();
            //record.topic();
            //record.partition();
            //record.key();
            //record.value();
            ThreadUtil.sleep(1000 * 1);
            log.info("处理完毕 {}", consumerRecord);
            i.incrementAndGet();
        });
    }
}