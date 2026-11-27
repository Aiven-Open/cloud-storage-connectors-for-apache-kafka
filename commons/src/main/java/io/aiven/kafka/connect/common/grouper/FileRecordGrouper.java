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

package io.aiven.kafka.connect.common.grouper;

/**
 * A {@link RecordGrouper} whose files may receive records across several writes. It lets a sink task release the
 * records already written to a file without changing the file's identity, ask whether a file has reached its record
 * limit, and mark a file as finished so that the next record starts a new one.
 */
public interface FileRecordGrouper extends RecordGrouper {

    /**
     * Releases the buffered records of a file without changing its identity. The records already released still count
     * towards the file's record limit, and the next matching record is assigned to the same file unless it is full or
     * has been completed.
     *
     * @param recordKey
     *            the file's record key
     */
    void clearFileBuffers(String recordKey);

    /**
     * @param recordKey
     *            the file's record key
     * @return whether the file has reached the configured maximum number of records
     */
    boolean isFileFull(String recordKey);

    /**
     * Marks the file as finished and releases its buffered records. The next matching record starts a new file, with
     * that record as its head record.
     *
     * @param recordKey
     *            the file's record key
     */
    void completeFile(String recordKey);
}
