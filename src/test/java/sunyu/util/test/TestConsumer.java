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
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
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
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //重新调整某主题，某个分区的偏移量
        kafkaConsumerUtil.seek("US_GENERAL", 0, 7927573);
    }


    @Test
    void t003() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //将某主题，某个分区的偏移量调整到最后
        kafkaConsumerUtil.seekToEnd("US_GENERAL", 0);
    }

    @Test
    void t004() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //将某主题，某个分区的偏移量调整到最前
        kafkaConsumerUtil.seekToBeginning("US_GENERAL", 0);
    }

    @Test
    void t005() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        //控制台debug查看某主题，某分区的偏移量情况
        kafkaConsumerUtil.showOffsets("US_GENERAL", 0);
    }

    @Test
    void t006() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .setTopics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
                .build();
        kafkaConsumerUtil.showPartitions("US_GENERAL");
    }

    @Test
    void t007() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.INSTANCE
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
}