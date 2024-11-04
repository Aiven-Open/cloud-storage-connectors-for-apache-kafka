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

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Extracts data from a {@link SinkRecord}. The actual data extracted is implementation specific
 */
public interface DataExtractor {

    /**
     * Extracts data from a {@link SinkRecord}.
     *
     * @param record the record to extract data from
     * @return the extracted data
     */
    Object extractDataFrom(final SinkRecord record);
}
