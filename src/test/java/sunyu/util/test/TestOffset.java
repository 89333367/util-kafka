package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaOffsetUtil;

public class TestOffset {
    Log log = LogFactory.get();

    @Test
    void t001() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        //获取主题与分区信息
        for (TopicPartition topicPartition : kafkaOffsetUtil.getTopicPartitions()) {
            log.info("{}", topicPartition);
        }

        //程序关闭前回收资源
        kafkaOffsetUtil.close();
    }

    @Test
    void t002() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        //获取每个主题每个分区的偏移量
        for (TopicPartition topicPartition : kafkaOffsetUtil.getTopicPartitions()) {
            long earliestOffset = kafkaOffsetUtil.getEarliestOffset(topicPartition);
            long currentOffset = kafkaOffsetUtil.getCurrentOffset(topicPartition);
            long latestOffset = kafkaOffsetUtil.getLatestOffset(topicPartition);
            log.info("{} Earliest:{} Current:{} Latest:{}", topicPartition, earliestOffset, currentOffset, latestOffset);
        }

        //程序关闭前回收资源
        kafkaOffsetUtil.close();
    }

    @Test
    void t003() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        for (TopicPartition topicPartition : kafkaOffsetUtil.getTopicPartitions()) {
            //改变当前组的偏移量
            kafkaOffsetUtil.seekAndCommit(topicPartition, 0);
        }

        //程序关闭前回收资源
        kafkaOffsetUtil.close();
    }


    @Test
    void t004() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        //修复当前组的偏移量
        kafkaOffsetUtil.fixCurrentOffsets();

        //程序关闭前回收资源
        kafkaOffsetUtil.close();
    }

    @Test
    void t005() {
        KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        log.info("{}", kafkaOffsetUtil.getKafkaParamsMap());
        log.info("{}", kafkaOffsetUtil.getCurrentOffsets());

        //程序关闭前回收资源
        kafkaOffsetUtil.close();
    }


}
