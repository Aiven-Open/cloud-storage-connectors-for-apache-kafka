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

package io.aiven.kafka.connect.common.output;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.kafka.connect.sink.SinkRecord;

public interface OutputStreamWriter {

    default void startWriting(final OutputStream outputStream) throws IOException {
    }

    default void writeRecordsSeparator(final OutputStream outputStream) throws IOException {
    }

    void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException;

    default void stopWriting(final OutputStream outputStream) throws IOException {
    }

}
