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
    <version>kafka-clients-0.9.0.1_v1.0</version>
</dependency>
```

## kafka消费者

```java

@Test
void t007() {
    KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
            //.topics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
            .topic("GENERAL_MSG")
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .groupId("test_group_kafka_consumer_util")
            .build();//全局只要定义一个即可
    kafkaConsumerUtil.pollRecord(consumerRecord -> {
        log.info("开始处理 {}", consumerRecord);
        //处理消息，如果这里没有抛异常，则消息会自动提交offset，如果这里 throw Exception，那么这条消息不会提交offset，下次还会拉取回来
        //record.offset();
        //record.topic();
        //record.partition();
        //record.key();
        //record.value();
        ThreadUtil.sleep(1000 * 3);
        log.info("处理完毕 {}", consumerRecord);
    });
}

@Test
void t001() {
    KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .groupId("test_group_kafka_consumer_util")
            .topics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
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
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .groupId("test_group_kafka_consumer_util")
            .topics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
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
void t008() {
    AtomicInteger i = new AtomicInteger();
    Properties config = new Properties();
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase()); // OffsetResetStrategy.LATEST.name().toLowerCase()
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_kafka_consumer_util");
    KafkaConsumerUtil kafkaConsumerUtil = KafkaConsumerUtil.builder()
            //.topics(Arrays.asList("US_GENERAL", "US_GENERAL_FB", "DS_RESPONSE_FB"))
            .topic("GENERAL_MSG")
            .build(config);//全局只要定义一个即可
    kafkaConsumerUtil.pollRecord(consumerRecord -> {
        if (i.get() >= 5) {
            kafkaConsumerUtil.close();
            return;
        }
        log.info("开始处理 {}", consumerRecord);
        //处理消息，如果这里没有抛异常，则消息会自动提交offset，如果这里 throw Exception，那么这条消息不会提交offset，下次还会拉取回来
        //record.offset();
        //record.topic();
        //record.partition();
        //record.key();
        //record.value();
        ThreadUtil.sleep(1000 * 1);
        log.info("处理完毕 {}", consumerRecord);
        i.incrementAndGet();
    });
}
```

## kafka生产者

```java

@Test
void 同步发送消息() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值");
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 同步发送消息并且自己处理metadata和exception() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    kafkaProducerUtil.sendSync("主题", "键，这里可以为null", "值", (metadata, exception) -> {
        // 这里你可以自己处理 metadata 和 exception 异常信息
    });
    kafkaProducerUtil.flush();//如果想消息立刻发送，不缓存，那么调用这句话，否则消息会缓存一下，隔一会才发送
}

@Test
void 异步发送消息() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
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
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
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
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    for (int i = 0; i < 10000; i++) {
        //这里发送消息
    }
    kafkaProducerUtil.flush();//批量发送后，调用一次flush即可，不需要在每一次循环中调用，减少网络交互
}

@Test
void 关闭整个项目() {
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .build();
    //这里发送消息

    //如果整个项目需要关闭，调用close释放资源
    kafkaProducerUtil.close();
}

@Test
void 构建传参() {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092");
    KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
            .build(config);

    kafkaProducerUtil.sendSync("test_topic", "key1", "value1");

    //如果整个项目需要关闭，调用close释放资源
    kafkaProducerUtil.close();
}
```

### kafka偏移量使用

```java

@Test
void t001() {
    KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .groupId("test_group_kafka_consumer_util")
            .build();
    //重新调整某主题，某个分区的偏移量
    kafkaOffsetUtil.seek("US_GENERAL", 0, 7927573);
}


@Test
void t002() {
    KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder()
            .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
            .groupId("test_group_kafka_consumer_util")
            .build();
    //将某主题，某个分区的偏移量调整到最后
    kafkaOffsetUtil.seekToEnd("US_GENERAL", 0);
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
    kafkaOffsetUtil.showPartitions("US_GENERAL");
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
```