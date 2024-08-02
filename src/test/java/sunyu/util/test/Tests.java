package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

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
}
