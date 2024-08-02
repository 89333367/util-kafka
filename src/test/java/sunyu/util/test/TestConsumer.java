package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaConsumerUtil;

import java.util.Arrays;

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
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
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
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setTopic("GENERAL_MSG")
                .setBootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        kafkaConsumerUtil.pollRecord(consumerRecord -> {
            log.info("开始处理 {}", consumerRecord);
            ThreadUtil.sleep(1000 * 60);
            log.info("处理完毕 {}", consumerRecord);
        });
    }
}