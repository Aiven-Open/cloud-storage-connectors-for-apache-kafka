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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks, per file, how many records have been assigned to it and whether it has been explicitly completed. The counts
 * are independent of the records still buffered in memory, so a grouper can release a file's buffer after an interim
 * write without changing the file's identity.
 */
final class FileRotationTracker {

    private final Integer maxRecordsPerFile;

    private final Map<String, Integer> fileRecordCounts = new HashMap<>();

    private final Set<String> completedFiles = new HashSet<>();

    /**
     * @param maxRecordsPerFile
     *            the maximum number of records per file ({@code null} for unlimited).
     */
    FileRotationTracker(final Integer maxRecordsPerFile) {
        this.maxRecordsPerFile = maxRecordsPerFile;
    }

    void recordAdded(final String recordKey) {
        fileRecordCounts.merge(recordKey, 1, Integer::sum);
    }

    /**
     * Whether the next record must start a new file instead of being assigned to the given one.
     */
    boolean shouldRotate(final String recordKey) {
        return completedFiles.contains(recordKey) || isFileFull(recordKey);
    }

    boolean isFileFull(final String recordKey) {
        return maxRecordsPerFile != null && fileRecordCounts.getOrDefault(recordKey, 0) >= maxRecordsPerFile;
    }

    void completeFile(final String recordKey) {
        Objects.requireNonNull(recordKey, "record key cannot be null");
        completedFiles.add(recordKey);
    }

    // Forgets a file once a new file has replaced it for the same topic/partition (and key).
    void remove(final String recordKey) {
        fileRecordCounts.remove(recordKey);
        completedFiles.remove(recordKey);
    }

    void clear() {
        fileRecordCounts.clear();
        completedFiles.clear();
    }
}
