package sunyu.util;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface ProducerCallback {
    void exec(RecordMetadata metadata, Exception exception);
}
