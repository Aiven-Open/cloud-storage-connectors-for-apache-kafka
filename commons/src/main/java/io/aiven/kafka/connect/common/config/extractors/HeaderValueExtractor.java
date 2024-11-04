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

package io.aiven.kafka.connect.common.config.extractors;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A DataExtractor that extracts the value of a header from a SinkRecord
 * If there are multiple headers with the same ane, then the last occurrence will be used
 * If the header does not exist, then the extraction will yield null
 */
public class HeaderValueExtractor implements DataExtractor {
    private final String headerKey;

    /**
     * Create a new HeaderValueExtractor
     *
     * @param headerKey the key of the header to extract.
     */
    public HeaderValueExtractor(final String headerKey) {
        this.headerKey = headerKey;
    }

    public Object extractDataFrom(final SinkRecord record) {
        final Header header = record.headers().lastWithName(headerKey);
        return header == null ? null : header.value();
    }
}
