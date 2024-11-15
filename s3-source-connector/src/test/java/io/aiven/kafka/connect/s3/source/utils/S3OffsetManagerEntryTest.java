package io.aiven.kafka.connect.s3.source.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3OffsetManagerEntryTest {

    private S3OffsetManagerEntry underTest;
    @BeforeEach
    public void setup() {
        underTest = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "topic", 1);
    }
    @Test
    public void testConstructorExpressedInMaps() {

        Map<String,Object> map = underTest.getProperties();
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
    public void testSetProperty() {
        underTest.setProperty("Foo", "bar");
        assertThat(underTest.getProperties().get("Foo")).isEqualTo("bar");
        assertThrows( IllegalArgumentException.class, () -> underTest.setProperty(S3OffsetManagerEntry.BUCKET, "someValue"));
        assertThrows( IllegalArgumentException.class, () -> underTest.setProperty(S3OffsetManagerEntry.OBJECT_KEY, "someValue"));
        assertThrows( IllegalArgumentException.class, () -> underTest.setProperty(S3OffsetManagerEntry.TOPIC, "someValue"));
        assertThrows( IllegalArgumentException.class, () -> underTest.setProperty(S3OffsetManagerEntry.PARTITION, "someValue"));
        assertThrows( IllegalArgumentException.class, () -> underTest.setProperty(S3OffsetManagerEntry.RECORD_COUNT, "someValue"));
    }

    @Test
    public void testConstructorExpressedInGetters() {
        assertThat(underTest.getKey()).isEqualTo("s3ObjectKey");
        assertThat(underTest.getTopic()).isEqualTo("topic");
        assertThat(underTest.getPartition()).isEqualTo(1);
        assertThat(underTest.getRecordCount()).isEqualTo(0L);
    }

    @Test
    public void testRecordCounterIncrementer() {
        assertThat(underTest.getRecordCount()).isEqualTo(0L);
        assertThat(underTest.shouldSkipRecord(0)).isFalse();
        underTest.incrementRecordCount();
        assertThat(underTest.getRecordCount()).isEqualTo(1L);
        assertThat(underTest.shouldSkipRecord(0)).isTrue();
        assertThat(underTest.shouldSkipRecord(1)).isFalse();
        assertThat(underTest.shouldSkipRecord(2)).isFalse();
    }
}
