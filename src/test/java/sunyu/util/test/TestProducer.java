package sunyu.util.test;

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

    @Test
    void t005() throws ExecutionException, InterruptedException {
        KafkaProducerUtil kafkaProducerUtil = KafkaProducerUtil.builder()
                .bootstrapServers("kafka005:9092,kafka015:9092,kafka016:9092")
                .build();

        kafkaProducerUtil.send("GENERAL_MSG", "5", "{\"ts\":\"20250318152143\",\"qos\":2,\"pType\":\"g4\",\"mType\":\"5_1\",\"data\":{\"startTime\":\"20250318152134\",\"startLon\":116.346111,\"startLat\":40.036786,\"did\":\"TEST202409209658\",\"code\":\"100_16_64_999\",\"userCode\":\"208\"}}").get();

        kafkaProducerUtil.flush();

        //项目关闭前要回收资源
        kafkaProducerUtil.close();
    }

}