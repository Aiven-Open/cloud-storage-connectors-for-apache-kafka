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

package io.aiven.kafka.connect.common.grouper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordsBatch {

    private int numberOfRecords;
    final private List<SinkRecord> sinkRecords;
    final private String filename;
    final private long recordCreationDate = System.currentTimeMillis();

    public SinkRecordsBatch(final String filename) {
        this.filename = filename;
        sinkRecords = new ArrayList<>();
        numberOfRecords = 0;
    }

    public SinkRecordsBatch(final String filename, final List<SinkRecord> sinkRecords) {
        this.filename = filename;
        this.sinkRecords = new ArrayList<>(sinkRecords);
        numberOfRecords = sinkRecords.size();
    }
    public SinkRecordsBatch(final String filename, final SinkRecord sinkRecord) {
        this.filename = filename;
        this.sinkRecords = new ArrayList<>();
        this.sinkRecords.add(sinkRecord);
        numberOfRecords = 1;
    }

    public void addSinkRecord(final SinkRecord sinkRecord) {
        this.sinkRecords.add(sinkRecord);
        this.numberOfRecords++;
    }

    public List<SinkRecord> getSinkRecords() {
        // Ensure access to the Sink Records can only be changed through the apis and not accidentally by another
        // process.
        return Collections.unmodifiableList(sinkRecords);
    }

    public void removeSinkRecords(final List<SinkRecord> sinkRecords) {
        this.sinkRecords.removeAll(sinkRecords);
    }

    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    public String getFilename() {
        return filename;
    }

    public long getRecordCreationDate() {
        return recordCreationDate;
    }

}
