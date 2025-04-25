package io.aiven.kafka.connect.azure.sink;


import com.azure.storage.blob.models.BlobItem;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.aiven.kafka.connect.azure.AzureStorage;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import org.apache.kafka.connect.connector.Connector;
import org.testcontainers.azure.AzuriteContainer;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AzureSinkStorage extends AzureStorage implements SinkStorage<BlobItem, String> {
    /**
     * Constructor.
     *
     * @param container the container to Azure read/write to.
     */
    public AzureSinkStorage(AzuriteContainer container) {
        super(container);
    }

    @Override
        public final String getAvroBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d.avro", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

@Override
    public final String getBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public final String getKeyBlobName(String prefix, String key, CompressionType compression) {
        final String result = String.format("%s%s", prefix, key);
        return result + compression.extension();
    }

    @Override
    public final String getNewBlobName(final String prefix, final String topicName, final int partition, final int startOffset, final CompressionType compression) {
        final String result = String.format("%s%s-%d-%d", prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    @Override
    public final String getTimestampBlobName(String prefix, String topicName, int partition, int startOffset) {
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
    public String getURLPathPattern(String topicName) {
        return  String.format("/%s/%s([\\-0-9]+)", containerAccessor.getContainerName(), topicName);
    }

    @Override
    public boolean enableProxy(Map<String, String> connectorConfig, WireMockServer proxy) {
        connectorConfig.put("aws.s3.endpoint", proxy.baseUrl());
        return true;
    }

    @Override
    public CompressionType getDefaultCompression() {
        return CompressionType.NONE;
    }

    @Override
    public Map<String, String> createSinkProperties(String prefix, String connectorName) {
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
