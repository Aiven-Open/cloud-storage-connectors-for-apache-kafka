/*
 * Copyright 2020 Aiven Oy
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

import java.nio.charset.StandardCharsets;

/**
 * The value returned by the KeyValueGenerator.
 */
public final class KeyValueMessage {
    /** The key as a string. */
    public final String key;
    /** the value as a string */
    public final String value;
    /** The partition this message was generated for  */
    public final int partition;
    /** The message index from the run */
    public final int idx;
    /** The epoch this message was generated for  */
    public final int epoch;

    /**
     * Constructor.
     * @param key the key string.
     * @param value the value string.
     * @param partition the partition.
     * @param idx the index from the run.
     * @param epoch the epoch.
     */
    public KeyValueMessage(final String key, final String value, final int partition, final int idx, final int epoch) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.idx = idx;
        this.epoch = epoch;
    }

    /**
     * Get the key as a string.
     * @return he key as a string.
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the key as a byte array.
     * @return the key as a byte array.
     */
    public byte[] getKeyBytes() {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get the value as a string.
     * @return he value as a string.
     */
    public String getValue() {
        return value;
    }

    /**
     * Get the value as a byte array.
     * @return the value as a byte array.
     */
    public byte[] getValueBytes() {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
