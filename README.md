# kafka工具类

## 描述

- KafkaConsumerUtil 消费者工具类
- KafkaProducerUtil 生产者工具类
- KafkaOffsetUtil 偏移量工具类

## 环境

- 适用于 jdk8 x64 及以上版本

## 引入依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-kafka</artifactId>
    <!-- {kafka-clients.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>0.9.0.1_1.0_jdk8_x64</version>
    <classifier>shaded</classifier>
</dependency>
```

## kafka消费者示例

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

## kafka生产者示例

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

### kafka偏移量使用示例

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

### 在Spark Streaming中的使用示例 (spark 1.6版本)
- pom.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>sunyu.demo</groupId>
    <artifactId>demo-spark-streaming-kafka</artifactId>
    <version>1.0.0</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.9.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.6.3</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.10</artifactId>
            <version>1.6.3</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka_2.10</artifactId>
            <version>1.6.3</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>cn.hutool</groupId>
            <artifactId>hutool-all</artifactId>
            <version>5.8.36</version>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-context</artifactId>
            <version>5.3.39</version>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-jdbc</artifactId>
            <version>5.3.39</version>
        </dependency>
        <dependency>
            <groupId>sunyu.util</groupId>
            <artifactId>util-kafka</artifactId>
            <!-- {kafka-clients.version}_{util.version}_{jdk.version}_{architecture.version} -->
            <version>0.9.0.1_1.0_jdk8_x64</version>
            <classifier>shaded</classifier>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- 跳过单元测试 -->
            <!-- https://central.sonatype.com/artifact/org.apache.maven.plugins/maven-surefire-plugin/versions -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
                <configuration>
                    <skip>true</skip>
                </configuration>
            </plugin>

            <!-- https://central.sonatype.com/artifact/org.apache.maven.plugins/maven-assembly-plugin/versions -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.7.1</version>
                <configuration>
                    <descriptors>
                        <descriptor>src/main/assembly/package.xml</descriptor>
                    </descriptors>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- Main.java
```java
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import relocation.org.apache.kafka.clients.consumer.OffsetAndMetadata;
import relocation.org.apache.kafka.common.TopicPartition;
import sunyu.util.KafkaOffsetUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Main {
    static Log log = LogFactory.get();
    static Props props = new Props("application.properties");
    static KafkaOffsetUtil kafkaOffsetUtil = KafkaOffsetUtil.builder().bootstrapServers(props.getStr("kafka")).groupId(props.getStr("kafka.group.id")).topic(props.getStr("kafka.topics")).build();

    public static void main(String[] args) {
        long seconds = 30;

        Map<String, String> kafkaParams = kafkaOffsetUtil.getKafkaParamsMap();
        log.info("[kafka参数] {}", kafkaParams);

        SparkConf sparkConf = new SparkConf();
        if (ArrayUtil.isEmpty(args)) {
            sparkConf.setAppName("spark streaming test");
            sparkConf.setMaster("local[*]");
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10");
        } else {
            log.info("命令行参数 : {}", JSONUtil.toJsonStr(args));
            seconds = Convert.toLong(args[0]);
        }

        Duration batchDuration = Durations.seconds(seconds);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, batchDuration);

        // 修复offsets
        kafkaOffsetUtil.fixCurrentOffsets();
        //获得当前的偏移量
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();
        Map<TopicPartition, Long> curOffsets = kafkaOffsetUtil.getCurrentOffsets();
        log.info("[curOffsets] {}", curOffsets);
        curOffsets.forEach((topicPartition, aLong) -> fromOffsets.put(new TopicAndPartition(topicPartition.topic(), topicPartition.partition()), aLong));
        log.info("[fromOffsets] {}", fromOffsets);

        KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, String[].class, kafkaParams, fromOffsets,
                        v1 -> new String[]{v1.topic(), v1.key(), v1.message()})
                .foreachRDD((VoidFunction<JavaRDD<String[]>>) javaRDD -> {
                    //循环一批数据
                    javaRDD.foreachPartition((VoidFunction<Iterator<String[]>>) iterator -> {
                        iterator.forEachRemaining(strings -> {
                            //log.info("[收到数据] topic:{} key:{} value:{}", strings[0], strings[1], strings[2]);
                            // todo 处理数据
                        });
                    });

                    // todo 提交offset
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (OffsetRange offsetRange : offsetRanges) {
                        offsets.put(new TopicPartition(offsetRange.topic(), offsetRange.partition()), new OffsetAndMetadata(offsetRange.untilOffset()));
                    }
                    kafkaOffsetUtil.commitOffsets(offsets);
                });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
```
- src/main/bin/stream-start.sh

```shell
#!/bin/bash

APP_NAME=bsr-streaming-kafka_
APP_VERSION=2016.06.06
APP_JAR=../lib/demo-spark-streaming-kafka-1.0.0.jar
MAIN_CLASS=sunyu.demo.Main

APP_ID=`yarn application -list |grep ${APP_NAME} |awk '{print $1}'`

if [ "${APP_ID}" != "" ] ; then
  echo `yarn application -kill ${APP_ID}`
fi

echo "Start Spark Application ${APP_NAME}${APP_VERSION} ...."

for jar in ../lib/*.jar
do
  if [ ${jar} != ${APP_JAR} ] ; then
    LIBJARS=${jar},${LIBJARS}
  fi
done

for file in ../resources/*; do
  if [ -f ${file} ]; then
    RESOURCES_FILES=${file},${RESOURCES_FILES}
  fi
done

spark-submit \
  --class ${MAIN_CLASS} \
  --master yarn \
  --deploy-mode cluster \
  --jars ${LIBJARS} \
  --files ${RESOURCES_FILES} \
  --conf spark.app.name=${APP_NAME}${APP_VERSION} \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1g \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=1g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.scheduler.mode=FIFO \
  --conf spark.streaming.concurrentJobs=1 \
  --conf spark.streaming.backpressure.enabled=false \
  --conf spark.streaming.kafka.maxRatePerPartition=10000 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  $APP_JAR \
  60
```

- src/main/assembly/package.xml

```xml
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.0.0 http://maven.apache.org/xsd/assembly-2.0.0.xsd">
    <id>package</id>
    <formats>
        <format>dir</format>
        <format>zip</format>
    </formats>
    <includeBaseDirectory>true</includeBaseDirectory>
    <fileSets>
        <fileSet>
            <directory>src/main/bin</directory>
            <outputDirectory>bin</outputDirectory>
            <directoryMode>755</directoryMode>
            <fileMode>751</fileMode>
        </fileSet>
        <fileSet>
            <directory>src/main/resources</directory>
            <outputDirectory>resources</outputDirectory>
            <directoryMode>755</directoryMode>
            <fileMode>644</fileMode>
        </fileSet>
    </fileSets>
    <dependencySets>
        <dependencySet>
            <outputDirectory>lib</outputDirectory>
            <directoryMode>755</directoryMode>
            <fileMode>644</fileMode>
            <scope>runtime</scope>
        </dependencySet>
    </dependencySets>
</assembly>
```

