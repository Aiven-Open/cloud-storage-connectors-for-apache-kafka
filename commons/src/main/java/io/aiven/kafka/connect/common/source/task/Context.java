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
 *            is a key unique to the object the context is being created about
 */
public class Context<K> {

    private String topic;
    private Integer partition;
    private Integer offset;
    private K storageKey;

    public Context(final K storageKey) {

        this.storageKey = storageKey;
    }

    public Optional<String> getTopic() {
        return Optional.ofNullable(topic);
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public Optional<Integer> getPartition() {
        return Optional.ofNullable(partition);
    }

    public void setPartition(final Integer partition) {
        this.partition = partition;
    }

    public Optional<K> getStorageKey() {
        return Optional.ofNullable(storageKey);
    }

    public void setStorageKey(final K storageKey) {
        this.storageKey = storageKey;
    }

    public Optional<Integer> getOffset() {
        return Optional.ofNullable(offset);
    }

    public void setOffset(final Integer offset) {
        this.offset = offset;
    }
}
