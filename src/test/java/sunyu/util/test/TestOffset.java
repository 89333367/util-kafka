package sunyu.util.test;

import org.junit.jupiter.api.Test;
import sunyu.util.KafkaOffsetUtil;

public class TestOffset {
    @Test
    void t001() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        //重新调整某主题，某个分区的偏移量
        kafkaOffsetUtil.seek("US_GENERAL", 0, 7927573);
    }


    @Test
    void t002() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_20240625")
                .build();
        //将某主题，某个分区的偏移量调整到最后
        kafkaOffsetUtil.seekToEnd("US_GENERAL", 0);
    }

    @Test
    void t003() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        //将某主题，某个分区的偏移量调整到最前
        kafkaOffsetUtil.seekToBeginning("US_GENERAL", 0);
    }

    @Test
    void t004() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        //控制台debug查看某主题，某分区的偏移量情况
        kafkaOffsetUtil.showOffsets("US_GENERAL", 0);
    }

    @Test
    void t005() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.showPartitions("US_GENERAL_NJ");
    }


    @Test
    void t006() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .setBootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .setGroupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.showPartitions("GENERAL_MSG");
    }
}
