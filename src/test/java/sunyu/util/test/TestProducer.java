package sunyu.util.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import sunyu.util.KafkaProducerUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TestProducer {
    Log log = LogFactory.get();

    @Test
    void t001() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("192.168.11.136:9092,192.168.11.138:9092,192.168.11.152:9092")
                .build();

        //发送消息
        kafkaProducerUtil.send("datafileexchange.task", "1971191895549308928", "{\"headers\":{\"userId\":\"1\",\"machineType\":\"1\",\"orgId\":\"1\",\"rootOrgId\":\"1\",\"opDevice\":\"1\",\"orgPath\":\"1#\",\"origin\":\"https://bcnytest.bcnyyun.com\",\"tenantId\":\"1\",\"token\":\"7d8168cf1690411d936d8bcaba0872d1\",\"realName\":\"于艳涛2233\",\"username\":\"Admin\",\"realIp\":\"10.20.1.1\",\"roleIds\":null},\"type\":\"add\",\"timestamp\":\"2025-09-25 20:35:45\",\"data\":{\"id\":\"1971191895549308928\"}}");

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

    @Test
    void t005() throws ExecutionException, InterruptedException {
        /*KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();*/

        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("cdh-kafka1:9092,cdh-kafka2:9092,cdh-kafka3:9092")
                .build();

        //kafkaProducerUtil.send("GENERAL_MSG", "5", "{\"ts\":\"20250322081923\",\"qos\":2,\"pType\":\"g4\",\"mType\":\"5_1\",\"data\":{\"startTime\":\"20250322081910\",\"startLon\":119.790028,\"startLat\":40.732983,\"did\":\"TESTDID0000000001\",\"code\":\"EC00000000O17000101\",\"userCode\":\"243\"}}").get();
        kafkaProducerUtil.send("GENERAL_MSG", "5", "{\"ts\":\"20250322081953\",\"qos\":2,\"pType\":\"g4\",\"mType\":\"5_2\",\"data\":{\"startTime\":\"20250322081910\",\"startLon\":119.790028,\"startLat\":40.732983,\"endLon\":119.790039,\"endLat\":40.732974,\"did\":\"TESTDID0000000001\",\"code\":\"EC00000000O17000101\",\"userCode\":\"243\",\"endTime\":\"20250322081939\"}}").get();

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

    @Test
    void t006() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();

        for (String line : FileUtil.readUtf8Lines("d:/tmp/FARM_WORK_OUTLINE-2.log")) {
            log.info("{}", line);
            kafkaProducerUtil.send("FARM_WORK_OUTLINE", null, line);
        }

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }


    @Test
    void t007() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();

        for (String line : FileUtil.readUtf8Lines("d:/tmp/FARM_FIX-1.log")) {
            log.info("{}", line);
            kafkaProducerUtil.send("FARM_FIX", null, line);
        }

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

    @Test
    void t008() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();

        for (String line : FileUtil.readLines("d:/tmp/5_2_0701.log", "utf16")) {
            if (StrUtil.isNotBlank(line) && JSONUtil.isTypeJSON(line)) {
                log.info("{}", line);
                kafkaProducerUtil.send("GENERAL_MSG", "5", line);
            }
        }

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

    @Test
    void t009() {
        List<String> l = new ArrayList<>();
        AtomicInteger i = new AtomicInteger();
        FileUtil.readLines(FileUtil.file("d:/tmp/sendalarmkafka/5_2.log"), CharsetUtil.charset("utf16"), (LineHandler) line -> {
            if (StrUtil.isNotBlank(line) && JSONUtil.isTypeJSON(line)) {
                log.info("{}", line);
                l.add(line);
                if (l.size() >= 1000) {
                    FileUtil.writeUtf8Lines(l, StrUtil.format("d:/tmp/sendalarmkafka/db/{}.txt", i.incrementAndGet()));
                    l.clear();
                }
            }
        });
        if (!l.isEmpty()) {
            FileUtil.writeUtf8Lines(l, StrUtil.format("d:/tmp/sendalarmkafka/db/{}.txt", i.incrementAndGet()));
            l.clear();
        }
    }

    @Test
    void t010() {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();
        for (int i = 877; i <= 877; i++) {
            String path = StrUtil.format("d:/tmp/sendalarmkafka/db/{}.txt", i);
            for (String line : FileUtil.readUtf8Lines(path)) {
                if (StrUtil.isNotBlank(line) && JSONUtil.isTypeJSON(line)) {
                    //log.info("{}", line);
                    kafkaProducerUtil.send("GENERAL_MSG", "5", line);
                }
            }
            kafkaProducerUtil.flush();
            log.info("{} 处理完毕", path);
            ThreadUtil.sleep(1000 * 20);
        }

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

}