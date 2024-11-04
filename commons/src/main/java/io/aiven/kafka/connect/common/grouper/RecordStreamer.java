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

package io.aiven.kafka.connect.common.grouper;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

/**
 * The interface for classes that associates {@link SinkRecord}s with files by some criteria.
 */
public interface RecordStreamer{
    /**
     * determine the logical grouping of the record
     *
     * @param record
     *            - record to group
     */
    String getStream(SinkRecord record);

    /**
     * determine the actual filename of the record
     *
     * @param record
     *            - record to drive the filename
     */
    String getFilename(SinkRecord record);
}
