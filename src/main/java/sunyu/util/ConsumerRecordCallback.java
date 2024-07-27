package sunyu.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerRecordCallback {
    void exec(ConsumerRecord<String, String> record);
}
