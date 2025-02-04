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

package io.aiven.kafka.connect.common.source.task;

import java.util.Optional;

/**
 * A Context which captures all the details about the source which are required to successfully send a source record
 * onto Kafka
 *
 * @param <K>
 *            is is the type/class of the key unique to the object the context is being created about
 */
public class Context<K extends Comparable<K>> {

    private String topic;
    private Integer partition;
    private Long offset;
    private K storageKey;

    public Context(final K storageKey) {

        this.storageKey = storageKey;
    }

    /**
     * Creates a defensive copy of the Context for use internally by the S3SourceRecord
     *
     * @param anotherContext
     *            The Context which needs to be copied
     */
    protected Context(final Context<K> anotherContext) {
        this.storageKey = anotherContext.storageKey;
        this.partition = anotherContext.partition;
        this.topic = anotherContext.topic;
        this.offset = anotherContext.offset;
    }

    public final Optional<String> getTopic() {
        return Optional.ofNullable(topic);
    }

    public final void setTopic(final String topic) {
        this.topic = topic;
    }

    public final Optional<Integer> getPartition() {
        return Optional.ofNullable(partition);
    }

    public final void setPartition(final Integer partition) {
        this.partition = partition;
    }

    public final Optional<K> getStorageKey() {
        return Optional.ofNullable(storageKey);
    }

    public final void setStorageKey(final K storageKey) {
        this.storageKey = storageKey;
    }

    public final Optional<Long> getOffset() {
        return Optional.ofNullable(offset);
    }

    public final void setOffset(final Long offset) {
        this.offset = offset;
    }
}
