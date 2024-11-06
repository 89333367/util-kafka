package sunyu.util.test;

import cn.hutool.core.util.IdUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaOffsetUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestOffset {
    Log log = LogFactory.get();

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
        kafkaOffsetUtil.offsetLatest("US_GENERAL").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }

    @Test
    void t005() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.offsetEarliest("US_GENERAL").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }


    @Test
    void t006() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        kafkaOffsetUtil.offsetCurrent("US_GENERAL").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }


    @Test
    void t007() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka005:9092,kafka015:9092,kafka016:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_kafka_consumer_util");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .build(config);

        // todo code

        kafkaOffsetUtil.close();
    }

    @Test
    void t008() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .groupId("test_group_kafka_consumer_util")
                .build();
        for (int i = 0; i < 10; i++) {
            kafkaOffsetUtil.seekToBeginning("GENERAL_MSG", i);
        }
    }


    @Test
    void t009() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("alter-farm-dev1")
                .build();
        //控制台debug查看某主题，某分区的偏移量情况
        kafkaOffsetUtil.offsetLatest("FARM_WORK_OUTLINE").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }

    @Test
    void t010() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("alter-farm-dev1")
                .build();
        kafkaOffsetUtil.offsetEarliest("FARM_WORK_OUTLINE").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }


    @Test
    void t011() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("alter-farm-dev1")
                .build();
        kafkaOffsetUtil.offsetCurrent("FARM_WORK_OUTLINE").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }


    @Test
    void t012() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("alter-farm-dev1")
                .build();
        for (int i = 0; i < 4; i++) {
            kafkaOffsetUtil.seekToBeginning("FARM_WORK_OUTLINE", i);
        }
    }

    @Test
    void t013() {
        String topic = "US_GENERAL";
        Properties config = new Properties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, IdUtil.fastSimpleUUID());//配置客户端id
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_commit_groupid");
        Consumer<?, ?> consumer = new KafkaConsumer<>(config);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        /*consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, 0);*/
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(123456));
        consumer.commitSync(offsets);
        consumer.close();
    }


    @Test
    void t014() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_commit_groupid")
                .build();
        kafkaOffsetUtil.offsetCurrent("US_GENERAL").forEach((topicPartition, offsetAndMetadata) -> log.info("{} {}", topicPartition, offsetAndMetadata));
    }
}
