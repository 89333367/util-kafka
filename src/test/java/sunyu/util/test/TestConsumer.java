package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
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
}