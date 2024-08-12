package sunyu.util.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaOffsetUtil;

import java.util.Properties;

public class TestOffset {
    @Test
    void t001() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        //重新调整某主题，某个分区的偏移量
        kafkaOffsetUtil.seek("GENERAL_MSG", 0, 17236000);
    }


    @Test
    void t002() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        //将某主题，某个分区的偏移量调整到最后
        kafkaOffsetUtil.seekToEnd("GENERAL_MSG", 0);
    }

    @Test
    void t003() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        //将某主题，某个分区的偏移量调整到最前
        kafkaOffsetUtil.seekToBeginning("US_GENERAL", 0);
    }

    @Test
    void t004() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        //控制台debug查看某主题，某分区的偏移量情况
        kafkaOffsetUtil.showOffsets("US_GENERAL", 0);
    }

    @Test
    void t005() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.showPartitions("US_GENERAL_NJ");
    }


    @Test
    void t006() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.showPartitions("GENERAL_MSG");
    }


    @Test
    void t007() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_kafka_consumer_util");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .build(config);
        kafkaOffsetUtil.showPartitions("GENERAL_MSG");
        kafkaOffsetUtil.close();
    }
}
