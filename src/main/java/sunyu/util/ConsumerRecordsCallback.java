package sunyu.util;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerRecordsCallback {
    void exec(ConsumerRecords<String, String> records);
}
