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

package io.aiven.kafka.connect.s3.source.utils;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3OffsetManagerEntryTest {

    private S3OffsetManagerEntry underTest;
    @BeforeEach
    public void setup() {
        underTest = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "topic", 1);
    }
    @Test
    void testConstructorExpressedInMaps() {

        Map<String, Object> map = underTest.getProperties();
        assertThat(map.get(S3OffsetManagerEntry.BUCKET)).isEqualTo("bucket");
        assertThat(map.get(S3OffsetManagerEntry.OBJECT_KEY)).isEqualTo("s3ObjectKey");
        assertThat(map.get(S3OffsetManagerEntry.TOPIC)).isEqualTo("topic");
        assertThat(map.get(S3OffsetManagerEntry.PARTITION)).isEqualTo(1);
        assertThat(map.get(S3OffsetManagerEntry.RECORD_COUNT)).isEqualTo(0L);

        map = underTest.getManagerKey().getPartitionMap();
        assertThat(map.get(S3OffsetManagerEntry.BUCKET)).isEqualTo("bucket");
        assertThat(map.get(S3OffsetManagerEntry.TOPIC)).isEqualTo("topic");
        assertThat(map.get(S3OffsetManagerEntry.PARTITION)).isEqualTo(1);
    }

    @Test
    void testSetProperty() {
        underTest.setProperty("Foo", "bar");
        assertThat(underTest.getProperties().get("Foo")).isEqualTo("bar");
        assertThrows(IllegalArgumentException.class,
                () -> underTest.setProperty(S3OffsetManagerEntry.BUCKET, "someValue"));
        assertThrows(IllegalArgumentException.class,
                () -> underTest.setProperty(S3OffsetManagerEntry.OBJECT_KEY, "someValue"));
        assertThrows(IllegalArgumentException.class,
                () -> underTest.setProperty(S3OffsetManagerEntry.TOPIC, "someValue"));
        assertThrows(IllegalArgumentException.class,
                () -> underTest.setProperty(S3OffsetManagerEntry.PARTITION, "someValue"));
        assertThrows(IllegalArgumentException.class,
                () -> underTest.setProperty(S3OffsetManagerEntry.RECORD_COUNT, "someValue"));
    }

    @Test
    void testConstructorExpressedInGetters() {
        assertThat(underTest.getKey()).isEqualTo("s3ObjectKey");
        assertThat(underTest.getTopic()).isEqualTo("topic");
        assertThat(underTest.getPartition()).isEqualTo(1);
        assertThat(underTest.getRecordCount()).isEqualTo(0L);
    }

    @Test
    void testRecordCounterIncrementer() {
        assertThat(underTest.getRecordCount()).isEqualTo(0L);
        assertThat(underTest.shouldSkipRecord(0)).isFalse();
        underTest.incrementRecordCount();
        assertThat(underTest.getRecordCount()).isEqualTo(1L);
        assertThat(underTest.shouldSkipRecord(0)).isTrue();
        assertThat(underTest.shouldSkipRecord(1)).isFalse();
        assertThat(underTest.shouldSkipRecord(2)).isFalse();
    }
}
