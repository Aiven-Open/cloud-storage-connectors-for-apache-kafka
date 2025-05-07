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

package io.aiven.kafka.connect.common.source.impl;

import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.connect.common.source.OffsetManager;

import com.google.common.base.Objects;

/**
 * An implementation of OffsetManagerEntry. This entry has 3 values stored in the map.
 */
public class ExampleOffsetManagerEntry implements OffsetManager.OffsetManagerEntry<ExampleOffsetManagerEntry> {
    public Map<String, Object> data;

    private int recordCount;

    private static final String KEY = "key";
    private static final String GROUPING_KEY = "groupingKey";
    private static final String RECORD_COUNT = "recordCount";

    /**
     * Constructor.
     *
     * @param nativeKey
     *            The native Key.
     *
     * @param grouping
     *            An grouping division
     */
    public ExampleOffsetManagerEntry(final String nativeKey, final String grouping) {
        this();
        data.put(KEY, nativeKey);
        data.put(GROUPING_KEY, grouping);
    }

    /**
     * Constructor.
     */
    private ExampleOffsetManagerEntry() {
        data = new HashMap<>();
    }

    /**
     * A constructor.
     *
     * @param properties
     *            THe data map to use.
     */
    public ExampleOffsetManagerEntry(final Map<String, Object> properties) {
        this();
        data.putAll(properties);
        if (data.containsKey(RECORD_COUNT)) {
            recordCount = getInt(RECORD_COUNT);
        }
    }

    @Override
    public ExampleOffsetManagerEntry fromProperties(final Map<String, Object> properties) {
        return new ExampleOffsetManagerEntry(properties);
    }

    @Override
    public Map<String, Object> getProperties() {
        data.put(RECORD_COUNT, recordCount);
        return data;
    }

    @Override
    public Object getProperty(final String key) {
        return data.get(key);
    }

    @Override
    public void setProperty(final String key, final Object value) {
        data.put(key, value);
    }

    @Override
    public OffsetManager.OffsetManagerKey getManagerKey() {
        return new OffsetManager.OffsetManagerKey(Map.of(KEY, data.get(KEY), GROUPING_KEY, data.get(GROUPING_KEY)));
    }

    @Override
    public void incrementRecordCount() {
        recordCount++;
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof ExampleOffsetManagerEntry) {
            return this.compareTo((ExampleOffsetManagerEntry) other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getProperty(KEY), getProperty(GROUPING_KEY));
    }

    @Override
    public int compareTo(final ExampleOffsetManagerEntry other) {
        if (other == this) { // NOPMD
            return 0;
        }
        int result = ((String) getProperty(KEY)).compareTo((String) other.getProperty(KEY));
        if (result == 0) {
            result = ((String) getProperty(GROUPING_KEY)).compareTo((String) other.getProperty(GROUPING_KEY));
            if (result == 0) {
                result = Long.compare(getRecordCount(), other.getRecordCount());
            }
        }
        return result;
    }
}
