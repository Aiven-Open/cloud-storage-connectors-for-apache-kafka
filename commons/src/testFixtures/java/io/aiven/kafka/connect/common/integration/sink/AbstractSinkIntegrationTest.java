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

public abstract class AbstractSinkIntegrationTest<N, K extends Comparable<K>> extends KafkaIntegrationTestBase {
    protected String prefix;
    protected KafkaManager kafkaManager;
    protected SinkStorage<N, K> sinkStorage;
    protected String connectorName;

    protected abstract SinkStorage<N, K> getSinkStorage();

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

    protected final Function<GenericRecord, String> mapF(final String key) {
        return r -> r.get(key).toString();
    }

}
