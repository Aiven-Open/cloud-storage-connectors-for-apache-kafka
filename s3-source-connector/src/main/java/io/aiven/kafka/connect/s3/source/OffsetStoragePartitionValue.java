/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.s3.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OffsetStoragePartitionValue implements Comparable<OffsetStoragePartitionValue> {

    public static final String ORIGINAL_OFFSET = "storedOriginalOffset";
    private final long offset;

    public OffsetStoragePartitionValue(final long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        this.offset = offset;
    }

    public static OffsetStoragePartitionValue fromOffsetMap(final long offset) {
        return new OffsetStoragePartitionValue(offset);
    }

    public static OffsetStoragePartitionValue fromOffsetMap(final Map<String, Object> map) {
        Objects.requireNonNull(map, "Input map cannot be null");
        final Object offsetValue = map.get(ORIGINAL_OFFSET);
        if (!(offsetValue instanceof Number)) {
            throw new IllegalArgumentException("Original offset must be a valid number");
        }
        return fromOffsetMap(((Number) offsetValue).longValue());
    }

    @Override
    public String toString() {
        return String.valueOf(offset);
    }

    @Override
    public int compareTo(final OffsetStoragePartitionValue other) {
        return Long.compare(this.offset, other.offset);
    }

    public Map<String, ?> asOffsetMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(ORIGINAL_OFFSET, offset);
        return map;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OffsetStoragePartitionValue other = (OffsetStoragePartitionValue) obj;
        return offset == other.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset);
    }
}
