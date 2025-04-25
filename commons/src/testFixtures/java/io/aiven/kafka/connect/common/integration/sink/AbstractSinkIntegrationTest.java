package io.aiven.kafka.connect.common.integration.sink;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.NativeInfo;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractSinkIntegrationTest<N, K extends Comparable<K>> extends AbstractKafkaIntegrationBase {
    protected String prefix;
    protected KafkaManager kafkaManager;
    protected SinkStorage<N, K> sinkStorage;

    protected abstract SinkStorage<N, K> getSinkStorage();
    protected String connectorName;


    final protected String createPrefix() {
        return "connector-test-" + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
    }

    @BeforeEach
    final void setUp() throws ExecutionException, InterruptedException, IOException {
        sinkStorage = getSinkStorage();
        prefix = sinkStorage.defaultPrefix();
        sinkStorage.createStorage();
        connectorName = getConnectorName(sinkStorage.getConnectorClass());
        kafkaManager = setupKafka(sinkStorage.getConnectorClass());
    }

    @AfterEach
    void removeStorage() {
        sinkStorage.removeStorage();
    }

    protected Map<String, String> createConfiguration(String... topics) {
        Map<String, String> config = sinkStorage.createSinkProperties(prefix, connectorName);
        config.put("name", connectorName);
        config.put("topics", topics.length == 1 ?  topics[0] : String.join(",", topics));
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        FileNameFragment.setter(config).fileCompression(sinkStorage.getDefaultCompression());
        return config;
    }

    protected final List<K> getNativeKeys() {
        return sinkStorage.getNativeStorage().stream().map(NativeInfo::getNativeKey).collect(Collectors.toList());
    }

    protected final byte[] readBytes(final K nativeKey, final CompressionType compression) throws IOException {
        try (InputStream inputStream = sinkStorage.getInputStream(nativeKey).get();
             InputStream decompressedStream = compression.decompress(inputStream);
             ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
            IOUtils.copy(decompressedStream, decompressedBytes);
            decompressedStream.close();
            return decompressedBytes.toByteArray();
        }
    }

    protected final K[] arrayOf(K... elements) {
        return  Arrays.copyOf(elements, elements.length);
    }

    protected final Function<GenericRecord, String> mapF(final String key) {
        return r -> r.get(key).toString();
    }



}
