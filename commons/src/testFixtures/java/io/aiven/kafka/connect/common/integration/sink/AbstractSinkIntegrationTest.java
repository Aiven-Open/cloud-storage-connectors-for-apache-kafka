/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.integration.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.integration.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.NativeInfo;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Defines the general functionality required to run the sink integration tests.
 *
 * @param <N>
 *            the native storage object type
 * @param <K>
 *            the native storage key type.
 */
public abstract class AbstractSinkIntegrationTest<N, K extends Comparable<K>> extends KafkaIntegrationTestBase {
    protected String prefix;
    protected KafkaManager kafkaManager;
    protected SinkStorage<N, K> sinkStorage;
    protected String connectorName;

    /**
     * Get the SinkStorage implementation.
     *
     * @return the SinkStorage implementation for the connector under test.
     */
    protected abstract SinkStorage<N, K> getSinkStorage();

    /**
     * Creates a prefix based on timestamp for use in testing.
     *
     * @return the created prefix
     */
    final protected String createPrefix() {
        return "connector-test-" + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
    }

    /**
     * Sets up the sink storage including starting the kafka manager for the connector and definint the connector name.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @BeforeEach
    final void setUp() throws ExecutionException, InterruptedException, IOException {
        sinkStorage = getSinkStorage();
        prefix = sinkStorage.defaultPrefix();
        sinkStorage.createStorage();
        connectorName = getConnectorName(sinkStorage.getConnectorClass());
        kafkaManager = setupKafka(sinkStorage.getConnectorClass());
    }

    /**
     * Cleans up after test by removing the objects in the native storage.
     */
    @AfterEach
    void removeStorage() {
        sinkStorage.removeStorage();
    }

    /**
     * Creates the base configuration for a sink connector. Inncludes the connector name, topics and setting the max
     * tasks to 1. It also sets the schema registry URLs for the key and value converter.
     *
     * @param topics
     *            the list of topics to be created.
     * @return the configuration map.
     */
    protected Map<String, String> createConfiguration(final String... topics) {
        final Map<String, String> config = sinkStorage.createSinkProperties(prefix, connectorName);
        config.put("name", connectorName);
        config.put("topics", topics.length == 1 ? topics[0] : String.join(",", topics));
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        FileNameFragment.setter(config).fileCompression(sinkStorage.getDefaultCompression());
        return config;
    }

    /**
     * Get the list of native keys from the sink storage.
     *
     * @return the list of native keys.
     */
    protected final List<K> getNativeKeys() {
        return sinkStorage.getNativeStorage().stream().map(NativeInfo::getNativeKey).collect(Collectors.toList());
    }

    /**
     * Reads a file from native storage, optionally decompresses it, and returns the contents as a byte array.
     *
     * @param nativeKey
     *            the native key to read from the storage.
     * @param compression
     *            the compression that was applied to the storage.
     * @return the contents for the native file.
     * @throws IOException
     *             on read error.
     */
    protected final byte[] readBytes(final K nativeKey, final CompressionType compression) throws IOException {
        try (InputStream inputStream = sinkStorage.getInputStream(nativeKey).get();
                InputStream decompressedStream = compression.decompress(inputStream);
                ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
            IOUtils.copy(decompressedStream, decompressedBytes);
            decompressedStream.close();
            return decompressedBytes.toByteArray();
        }
    }

    /**
     * Convenience function to extract a value from a {@link GenericRecord} as a string.
     *
     * @param key
     *            the key value for the genertic record.
     * @return the value of the key as a string.
     */
    protected final Function<GenericRecord, String> mapF(final String key) {
        return r -> r.get(key).toString();
    }

}
