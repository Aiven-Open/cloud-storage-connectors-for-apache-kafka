package io.aiven.kafka.connect.common.integration.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class RecordProducer <K, V> {

    final KafkaProducer<K, V> producer;

    RecordProducer(final KafkaProducer<K, V> producer) {
        this.producer = producer;
    }

    /**
     * Sends a message in an async manner.  All messages are sent with a byte[] key and value type.
     * @param topicName the topic to send the message on.
     * @param partition the partition for the message.
     * @param key the key for the message.
     * @param value the value for the message.
     * @return A future that will return the {@link RecordMetadata} for the message.
     */
    public Future<RecordMetadata> sendMessageAsync(final String topicName, final int partition, final K key, final V value) {
        final ProducerRecord<K, V> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    public void close() {
        producer.close();
    }

    public void flush() {
        producer.flush();
    }
}
