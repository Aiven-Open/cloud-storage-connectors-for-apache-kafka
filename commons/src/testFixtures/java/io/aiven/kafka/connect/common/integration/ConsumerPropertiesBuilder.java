package io.aiven.kafka.connect.common.integration;

import io.aiven.kafka.connect.common.config.TransformerFragment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerPropertiesBuilder {
    final Properties props = new Properties();

    public ConsumerPropertiesBuilder(String bootstrapServers) {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("specific.avro.reader", "false");
    }

    public ConsumerPropertiesBuilder keyDeserializer(final Class<? extends Deserializer<?>> keyDeserializer) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getCanonicalName());
        return this;
    }

    public ConsumerPropertiesBuilder valueDeserializer(final Class<? extends Deserializer<?>> valueDeserializer) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getCanonicalName());
        return this;
    }

    public ConsumerPropertiesBuilder schemaRegistry(final String schemaRegistryUrl) {
        props.put(TransformerFragment.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        return this;
    }

    public Properties build() {
        return props;
    }
}
