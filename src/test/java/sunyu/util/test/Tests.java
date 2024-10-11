package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Tests {
    Log log = LogFactory.get();

    @Test
    void t001() {
        String topics = "a,b";
        log.debug("{}", Arrays.asList(topics.split(",")));

        topics = "a";
        log.debug("{}", Arrays.asList(topics.split(",")));
    }

    @Test
    void t002() {
        CronUtil.schedule("0/1 * * * * ? ", (Task) () -> {
            log.info("执行啦");
        });
        CronUtil.setMatchSecond(true);//开启秒级别定时任务
        CronUtil.start();
        if (!CronUtil.getScheduler().isStarted()) {
            CronUtil.start();
        }

        ThreadUtil.sleep(1000000);
    }


    @Test
    void t003() {
        if (!CronUtil.getScheduler().isStarted()) {
            CronUtil.setMatchSecond(true);
            CronUtil.start();
        }

        CronUtil.schedule("0/1 * * * * ? ", (Task) () -> {
            log.info("1执行啦");
        });


        if (!CronUtil.getScheduler().isStarted()) {
            CronUtil.setMatchSecond(true);
            CronUtil.start();
        }
        CronUtil.schedule("0/5 * * * * ? ", (Task) () -> {
            log.info("5执行啦");
        });

        ThreadUtil.sleep(1000000);
    }

    @Test
    void t004() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(0 + 1));
        offsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(1 + 1));
        offsets.put(new TopicPartition("t", 1), new OffsetAndMetadata(0 + 1));
        log.info("{}", offsets);
        log.info("{}", offsets.size());
    }

    @Test
    void t005() {
        LinkedBlockingQueue<String> q = new LinkedBlockingQueue<>();
        q.offer("1");
        q.offer("2");
        log.info("{}", q.size());
        q.clear();
        log.info("{}", q.size());
    }

}
