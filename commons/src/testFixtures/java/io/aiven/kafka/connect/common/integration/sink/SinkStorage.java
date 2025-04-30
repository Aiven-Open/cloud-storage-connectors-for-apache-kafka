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

import java.util.Map;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.StorageBase;

import com.github.tomakehurst.wiremock.WireMockServer;

/**
 * Accesses the native sink storage.
 * <p>Note -- Several of the methods here are used to create blob names for specific tests.  Once the extended formatting is available these can probably be compressed into
 * a single method using the file name template as an argument.</p>
 * @param <N> the native storage object type
 * @param <K> the native storage key type.
 */
public interface SinkStorage<N, K extends Comparable<K>> extends StorageBase<N, K> {
    /**
     * Get the native key for an avro based blob.
     * @param prefix the prefix for the storage location.
     * @param topicName the topic name for the storage location.
     * @param partition the partition for the storage location.
     * @param startOffset the start offset for the storage location.
     * @param compression the compression type for the data at the storage location.
     * @return a native key for the specified avro file.
     */
    K getAvroBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression);

    /**
     * Get the native key for a standard blob.
     * @param prefix the prefix for the storage location.
     * @param topicName the topic name for the storage location.
     * @param partition the partition for the storage location.
     * @param startOffset the start offset for the storage location.
     * @param compression the compression type for the data at the storage location.
     * @return a native key for the specified avro file.
     */
    K getBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression);

    /**
     * Get the native key for a standard blob.
     * @param prefix the prefix for the storage location.
     * @param key the key for the storage location.
     * @param compression the compression type for the data at the storage location.
     * @return a native key for the specified avro file.
     */
    K getKeyBlobName(String prefix, String key, CompressionType compression);

    /**
     * Get the native key for a blob with the new name format.
     * @param prefix the prefix for the storage location.
     * @param topicName the topic name for the storage location.
     * @param partition the partition for the storage location.
     * @param startOffset the start offset for the storage location.
     * @param compression the compression type for the data at the storage location.
     * @return a native key for the specified avro file.
     */
    K getNewBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression);

    /**
     * Return a native key with the format {@code prefixtopicName-partition-offset-yyyy-MM-dd}
     *
     * @param prefix
     * @param topicName
     * @param partition
     * @param startOffset
     * @return
     */
    K getTimestampBlobName(String prefix, String topicName, int partition, int startOffset);

    /**
     * Creates a map of the sink properties for the specific storage layer.
     * @param prefix the prefix for the files in the storage.
     * @param connectorName the name of the connector.
     * @return the map of configuration options specific to the storage layer.
     */
    Map<String, String> createSinkProperties(String prefix, String connectorName);

    /**
     * Get the URL of the sink storage endpoint.  This is used in testing to create a proxy that will return a HTTP 500 error
     * on every other call to ensure that the connector can handle network errors.
     * @return the URL of the sink storage endpoint
     */
    String getEndpointURL();

    /**
     * Gets the path to append to the result of {@link #getEndpointURL()} to create a request for the specific topic.
     * @param topicName the topic that is being written to.
     * @return the URL pattern to write to the specific topic.
     */
    String getURLPathPattern(String topicName);

    /**
     * Enables a proxy in front of the storage layer to test that the connector handles backend network errors.
     *
     * @param config
     *            the configuration for the sink.
     * @param wireMockServer
     *            A mock wire server to front the storage layer
     * @return @{code true} if supported, {@code false} otherwise.
     */
    boolean enableProxy(Map<String, String> config, WireMockServer wireMockServer);

    /**
     * Gets the default compression the backend uses when no other compression is specified.
     * @return the compression the backend uses when no other compression is specified.
     */
    CompressionType getDefaultCompression();

}
