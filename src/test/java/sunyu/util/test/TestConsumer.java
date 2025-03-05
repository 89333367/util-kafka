package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaConsumerUtil;

public class TestConsumer {
    Log log = LogFactory.get();

    @Test
    void t001() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        kafkaConsumerUtil.pollRecords(null, consumerRecords -> {
            log.info("收到消息 {}", consumerRecords.count());
            ThreadUtil.sleep(1000 * 30);//模拟处理时间
        });

        kafkaConsumerUtil.close();
    }

}