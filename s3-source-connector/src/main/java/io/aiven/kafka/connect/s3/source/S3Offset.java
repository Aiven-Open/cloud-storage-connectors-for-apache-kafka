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

import java.util.Map;
import java.util.Objects;

public class S3Offset implements Comparable<S3Offset> {

    private final String s3key;

    private final long offset;

    public S3Offset(final String s3key, final long offset) {
        this.s3key = s3key;
        this.offset = offset;
    }

    public static S3Offset from(final String s3key, final long offset) {
        return new S3Offset(s3key, offset);
    }

    public static S3Offset from(final Map<String, Object> map) {
        return from((String) map.get("s3key"), (Long) map.get("originalOffset"));
    }

    @Override
    public String toString() {
        return s3key + "@" + offset;
    }

    @Override
    public int compareTo(final S3Offset s3Offset) {
        final int compareTo = s3key.compareTo(s3Offset.s3key);
        return compareTo == 0 ? (int) (offset - s3Offset.offset) : compareTo;
    }

    // Overriding equals to ensure consistency with compareTo
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final S3Offset other = (S3Offset) obj;
        return offset == other.offset && Objects.equals(s3key, other.s3key);
    }

    // Overriding hashCode to match equals
    @Override
    public int hashCode() {
        return Objects.hash(s3key, offset);
    }
}
