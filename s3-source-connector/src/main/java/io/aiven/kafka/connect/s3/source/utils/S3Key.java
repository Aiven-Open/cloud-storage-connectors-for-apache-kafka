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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.Objects;

/**
 * The S3Key is a specific implementation of a Context Storage Key which implements the required methods, toString,
 * hashCode and equals. This allows all S3 Objects to be correctly processed into context.
 */
public final class S3Key {
    String storageKey;

    public S3Key(final String storageKey) {
        this.storageKey = storageKey;
    }

    /**
     * Returns a string representation of the storage key
     *
     * @return String representation of the storage key
     */
    @Override
    public String toString() {
        return storageKey;
    }

    /**
     * Implements and returns the HashCode for the S3 storagekey
     *
     * @return storageKey hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(storageKey);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final S3Key s3Key = (S3Key) other;
        return Objects.equals(storageKey, s3Key.storageKey);
    }
}
