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

public interface SinkStorage<N, K extends Comparable<K>> extends StorageBase<N, K> {

    K getAvroBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression);
    K getBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression);
    K getKeyBlobName(String prefix, String key, CompressionType compression);

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

    Map<String, String> createSinkProperties(String prefix, String connectorName);

    String getEndpointURL();

    String getURLPathPattern(String topicName);

    /**
     * Enable a proxy in front of the storage layer.
     *
     * @param config
     *            the configuration for the sink.
     * @param wireMockServer
     *            A mock wire server to front the storage layer
     * @return @{code true} if supported, {@code false} otherwise.
     */
    boolean enableProxy(Map<String, String> config, WireMockServer wireMockServer);

    CompressionType getDefaultCompression();

}
