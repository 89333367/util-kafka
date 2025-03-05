# kafka工具类

## 描述

* KafkaConsumerUtil 消费者工具类
* KafkaProducerUtil 生产者工具类
* KafkaOffsetUtil 偏移量工具类

## 环境

* 适用于 jdk8 x64 及以上版本

## 引入依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-kafka</artifactId>
    <!-- {kafka-clients.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>0.9.0.1_1.0_jdk8_x64</version>
</dependency>
```

## kafka消费者

```java
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


    @Test
    void t002() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        kafkaConsumerUtil.pollRecords(null, consumerRecords -> {
            log.info("收到消息 {}", consumerRecords.count());
            ThreadUtil.sleep(1000 * 30);//模拟处理时间
            throw new RuntimeException("模拟处理出现了异常");
        });

        kafkaConsumerUtil.close();
    }

    @Test
    void t003() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        kafkaConsumerUtil.pollRecord(record -> {
            log.info("收到消息 {}", record);
            ThreadUtil.sleep(1000 * 30);//模拟处理时间
        });

        kafkaConsumerUtil.close();
    }

    @Test
    void t004() {
        KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .groupId("test_group_kafka_consumer_util")
                .topic("GENERAL_MSG")
                .build();

        kafkaConsumerUtil.pollRecord(record -> {
            log.info("收到消息 {}", record);
            ThreadUtil.sleep(1000 * 30);//模拟处理时间
            throw new RuntimeException("模拟处理出现了异常");
        });

        kafkaConsumerUtil.close();
    }

}
```

## kafka生产者

```java
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaProducerUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestProducer {
    Log log = LogFactory.get();

    @Test
    void t001() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();

        //发送消息
        kafkaProducerUtil.send("主题", "键，这里可以为null", "值");

        //关闭之前，或者想让消息立刻发送，可以调用一下flush刷新缓存
        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

    @Test
    void t002() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .acks("all")
                .build();

        for (int i = 0; i < 100; i++) {
            kafkaProducerUtil.send("主题", "键，这里可以为null", "值" + i);
        }
        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

    @Test
    void t003() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();

        Future<RecordMetadata> future = kafkaProducerUtil.send("主题", "键，这里可以为null", "值");
        try {
            future.get();//等待消息发送完毕
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }


    @Test
    void t004() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();

        kafkaProducerUtil.getProducer()//获得原生producer操作
                .send(new ProducerRecord<>("", "", ""), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("消息发送失败", exception);
                    } else {
                        log.info("消息发送成功");
                    }
                });

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

}
```

### kafka偏移量使用

```java
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


}
```