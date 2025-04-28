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

package io.aiven.kafka.connect.azure.sink;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.azure.AzureStorage;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;

import com.azure.storage.blob.models.BlobItem;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.testcontainers.azure.AzuriteContainer;

public class AzureSinkStorage extends AzureStorage implements SinkStorage<BlobItem, String> {
    /**
     * Constructor.
     *
     * @param container
     *            the container to Azure read/write to.
     */
    public AzureSinkStorage(final AzuriteContainer container) {
        super(container);
    }

    @Override
    public final String getAvroBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d.avro", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public final String getBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public final String getKeyBlobName(final String prefix, final String key, final CompressionType compression) {
        final String result = String.format("%s%s", prefix, key);
        return result + compression.extension();
    }

    @Override
    public final String getNewBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public final String getTimestampBlobName(final String prefix, final String topicName, final int partition,
            final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s-%s-%s", prefix, topicName, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy")), time.format(DateTimeFormatter.ofPattern("MM")),
                time.format(DateTimeFormatter.ofPattern("dd")));
    }

    @Override
    public final String getEndpointURL() {
        return containerAccessor.getContainerUrl();
    }

    @Override
    public String getURLPathPattern(final String topicName) {
        return String.format("/%s/%s([\\-0-9]+)", containerAccessor.getContainerName(), topicName);
    }

    @Override
    public boolean enableProxy(final Map<String, String> connectorConfig, final WireMockServer proxy) {
        connectorConfig.put("aws.s3.endpoint", proxy.baseUrl());
        return true;
    }

    @Override
    public CompressionType getDefaultCompression() {
        return CompressionType.NONE;
    }

    @Override
    public Map<String, String> createSinkProperties(final String prefix, final String connectorName) {
        final Map<String, String> config = createConnectorConfig(connectorName);
        config.put("connector.class", getConnectorClass().getName());
        config.put(AzureBlobSinkConfig.FILE_NAME_PREFIX_CONFIG, prefix);
        return config;
    }

    @Override
    public final Class<? extends Connector> getConnectorClass() {
        return AzureBlobSinkConnector.class;
    }
}
