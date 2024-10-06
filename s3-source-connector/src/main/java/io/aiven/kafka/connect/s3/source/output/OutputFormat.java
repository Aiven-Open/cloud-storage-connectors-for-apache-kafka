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

package io.aiven.kafka.connect.s3.source.output;

public enum OutputFormat {
    AVRO("avro"), PARQUET("parquet"), JSON("json"), BYTES("bytes");

    private final String format;

    OutputFormat(final String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    public static OutputFormat valueOfFormat(final String outFormat) {
        for (final OutputFormat outputFormat : values()) {
            if (outputFormat.format.equalsIgnoreCase(outFormat)) {
                return outputFormat;
            }
        }
        throw new IllegalArgumentException("Unknown outFormat: " + outFormat);
    }

    @Override
    public String toString() {
        return format;
    }
}
