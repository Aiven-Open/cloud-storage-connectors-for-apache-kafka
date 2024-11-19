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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3OffsetManagerEntryTest {

    private static final String  DFLT_BUCKET = "bucket";
    private static final String  DFLT_KEY = "s3ObjectKey";
    private static final String  DFLT_TOPIC = "topic";
    private static final int  DFLT_PARTITION = 1;

    private S3OffsetManagerEntry underTest;
    @BeforeEach
    public void setup() {
        underTest = new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION);
    }
    @Test
    void testConstructorExpressedInMaps() {

        Map<String, Object> map = underTest.getProperties();
        assertThat(map.get(S3OffsetManagerEntry.BUCKET)).isEqualTo(DFLT_BUCKET);
        assertThat(map.get(S3OffsetManagerEntry.OBJECT_KEY)).isEqualTo(DFLT_KEY);
        assertThat(map.get(S3OffsetManagerEntry.TOPIC)).isEqualTo(DFLT_TOPIC);
        assertThat(map.get(S3OffsetManagerEntry.PARTITION)).isEqualTo(1);
        assertThat(map.get(S3OffsetManagerEntry.RECORD_COUNT)).isEqualTo(0L);

        map = underTest.getManagerKey().getPartitionMap();
        assertThat(map.get(S3OffsetManagerEntry.BUCKET)).isEqualTo(DFLT_BUCKET);
        assertThat(map.get(S3OffsetManagerEntry.TOPIC)).isEqualTo(DFLT_TOPIC);
        assertThat(map.get(S3OffsetManagerEntry.PARTITION)).isEqualTo(1);
    }

    @Test
    void testSetProperty() {
        underTest.setProperty("Foo", "bar");
        assertThat(underTest.getProperties().get("Foo")).isEqualTo("bar");
        assertThatThrownBy(() -> underTest.setProperty(S3OffsetManagerEntry.BUCKET, "someValue")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> underTest.setProperty(S3OffsetManagerEntry.OBJECT_KEY, "someValue")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> underTest.setProperty(S3OffsetManagerEntry.TOPIC, "someValue")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> underTest.setProperty(S3OffsetManagerEntry.PARTITION, "someValue")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> underTest.setProperty(S3OffsetManagerEntry.RECORD_COUNT, "someValue")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testConstructorExpressedInGetters() {
        assertThat(underTest.getKey()).isEqualTo(DFLT_KEY);
        assertThat(underTest.getTopic()).isEqualTo(DFLT_TOPIC);
        assertThat(underTest.getPartition()).isEqualTo(DFLT_PARTITION);
        assertThat(underTest.getRecordCount()).isEqualTo(0L);
    }


    public void testFromProperties() {
        assertThat(underTest.fromProperties(underTest.getProperties()).compareTo(underTest)).isEqualTo(0);
        assertThat(underTest.fromProperties(null)).isNull();
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fromPropertiesThrowingExceptionData")
    public void testFromPropertiesThrowingException(String name, Map<String, Object> map, Class<?> expected) {
        assertThatThrownBy(() -> underTest.fromProperties(map)).isInstanceOf(expected).as(name+" does not throw exception: "+expected.getName());
    }

    private static Map<String, Object> createPopulatedMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(S3OffsetManagerEntry.BUCKET, DFLT_BUCKET);
        map.put(S3OffsetManagerEntry.OBJECT_KEY, DFLT_KEY);
        map.put(S3OffsetManagerEntry.TOPIC, DFLT_TOPIC);
        map.put(S3OffsetManagerEntry.PARTITION, DFLT_PARTITION);
        map.put(S3OffsetManagerEntry.RECORD_COUNT, 0L);
        return map;
    }
    private static Stream<Arguments> fromPropertiesThrowingExceptionData() {
        String[] fields = {S3OffsetManagerEntry.BUCKET, S3OffsetManagerEntry.OBJECT_KEY, S3OffsetManagerEntry.TOPIC, S3OffsetManagerEntry.PARTITION, S3OffsetManagerEntry.RECORD_COUNT};
        List<Arguments> lst = new ArrayList<>();

        for (String field : fields) {
            Map<String, Object> map = createPopulatedMap();
            map.remove(field);
            lst.add(Arguments.of("Missing " + field, map, IllegalArgumentException.class));
        }
        return lst.stream();
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("compareToData")
    void testCompareTo(String name, S3OffsetManagerEntry other, int value) {
        assertThat(other.compareTo(underTest)).as("Failure in "+name).isEqualTo(value);
    }

    private static Stream<Arguments> compareToData() {
        List<Arguments> lst = new ArrayList<>();

        lst.add(Arguments.of("Same values", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION), 0));
        lst.add(Arguments.of("Bucket before", new S3OffsetManagerEntry(DFLT_BUCKET.substring(0, DFLT_BUCKET.length()-1), DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION), -1));
        lst.add(Arguments.of("Bucket after", new S3OffsetManagerEntry(DFLT_BUCKET+"5", DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION), 1));
        lst.add(Arguments.of("Key before", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY.substring(0, DFLT_KEY.length()-1), DFLT_TOPIC, DFLT_PARTITION), -1));
        lst.add(Arguments.of("Key after", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY+"5", DFLT_TOPIC, DFLT_PARTITION), 1));
        lst.add(Arguments.of("Topic before", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC.substring(0, DFLT_TOPIC.length()-1), DFLT_PARTITION), -1));
        lst.add(Arguments.of("Topic after", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC+"5", DFLT_PARTITION), 1));
        lst.add(Arguments.of("Partition before", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION-1), -1));
        lst.add(Arguments.of("Partition after", new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION+1), 1));

        S3OffsetManagerEntry entry = new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION);
        Map<String, Object> map = entry.getProperties();
        map.put(S3OffsetManagerEntry.RECORD_COUNT, -1L);
        entry = entry.fromProperties(map);
        lst.add(Arguments.of("Record count before", entry, -1));

         entry = new S3OffsetManagerEntry(DFLT_BUCKET, DFLT_KEY, DFLT_TOPIC, DFLT_PARTITION);
        entry.incrementRecordCount();
        lst.add(Arguments.of("Record count after", entry, 1));
        return lst.stream();
    }
}
