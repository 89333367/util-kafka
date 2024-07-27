# kafka工具类

> 适用于 kafka 0.9.x - kafka 1.x
> 
> 适用于 jdk8+

> 引入依赖
```xml
<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-kafka</artifactId>
    <version>kafka-clients-0.9.0.1_v1.0</version>
</dependency>
```
## kafka消费者
```java
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
```


## kafka生产者
```java
@Test
void 同步发送消息() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值");
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 同步发送消息并且自己处理metadata和exception() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值", (metadata, exception) -> {
        // 这里你可以自己处理 metadata 和 exception 异常信息
    });
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 异步发送消息() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    Future<RecordMetadata> recordMetadataFuture = kafkaProducerUtil.sendAsync("主题", "键，这里可以为null", "值");
    try {
        recordMetadataFuture.get();
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } catch (ExecutionException e) {
        throw new RuntimeException(e);
    }
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 异步发送消息并且自己处理metadata和exception() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    Future<RecordMetadata> recordMetadataFuture = kafkaProducerUtil.sendAsync("主题", "键，这里可以为null", "值", (metadata, exception) -> {
        // 这里你可以自己处理 metadata 和 exception 异常信息
    });
    try {
        recordMetadataFuture.get();
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } catch (ExecutionException e) {
        throw new RuntimeException(e);
    }
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 批量发送消息() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    for (int i = 0; i < 10000; i++) {
        //这里发送消息
    }
    kafkaProducerUtil.flush();//批量发送后，调用一次flush即可，不需要在每一次循环中调用，减少网络交互
}

@Test
void 关闭整个项目() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.INSTANCE
            .setBootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    //这里发送消息

    //如果整个项目需要关闭，调用close释放资源
    kafkaProducerUtil.close();
}
```