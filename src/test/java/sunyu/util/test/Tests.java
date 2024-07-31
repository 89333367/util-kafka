package sunyu.util.test;

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
}
