package io.aiven.kafka.connect.common.integration.sink;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.StorageBase;

import java.util.Map;

public interface SinkStorage<N, K extends Comparable<K>> extends StorageBase<N, K> {

    K getAvroBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
                                         final CompressionType compression);
    K getBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
                                     final CompressionType compression);
    K getKeyBlobName(final String prefix, final String key, final CompressionType compression);

    K getNewBlobName(final String prefix, final String topicName, final int partition, final int startOffset,
                                        final CompressionType compression);

    /**
     * Return a native key with the format {@code prefixtopicName-partition-offset-yyyy-MM-dd}
     * @param prefix
     * @param topicName
     * @param partition
     * @param startOffset
     * @return
     */
    K getTimestampBlobName(final String prefix, final String topicName, final int partition, final int startOffset);

    Map<String, String> createSinkProperties(String prefix, String connectorName);

    String getEndpointURL();

    String getURLPathPattern(String topicName);

    /**
     * Enable a proxy in front of the storage layer.
     * @param config the configuration for the sink.
     * @param wireMockServer A mock wire server to front the storage layer
     * @return @{code true} if supported, {@code false} otherwise.
     */
    boolean enableProxy(Map<String, String> config, WireMockServer wireMockServer);

    CompressionType getDefaultCompression();

}
