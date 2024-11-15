package io.aiven.kafka.connect.s3.source.utils;

import io.aiven.kafka.connect.s3.source.input.Transformer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3SourceRecordTest {

    @Test
    void testCreateSourceRecord() {
        Converter valueConverter = mock(Converter.class);
        Converter keyConverter = mock(Converter.class);
        S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "test-topic", 1);
        final S3SourceRecord s3SourceRecord = new S3SourceRecord(offsetManagerEntry, "mock-key".getBytes(StandardCharsets.UTF_8), "mock-value".getBytes(StandardCharsets.UTF_8));

        when(valueConverter.toConnectData(anyString(), any()))
                .thenReturn(new SchemaAndValue(Schema.BYTES_SCHEMA, "value-converted"));

        when(keyConverter.toConnectData(anyString(), any()))
                .thenReturn(new SchemaAndValue(null, "key-converted"));

        SourceRecord sourceRecord = s3SourceRecord.getSourceRecord(Optional.of(keyConverter), valueConverter);
        assertThat(sourceRecord.key()).isEqualTo("key-converted");
        assertThat(sourceRecord.value()).isEqualTo("value-converted");
        assertThat(sourceRecord.sourcePartition()).isEqualTo(offsetManagerEntry.getManagerKey().getPartitionMap());
        assertThat(sourceRecord.kafkaPartition()).isEqualTo(offsetManagerEntry.getPartition());
        assertThat(sourceRecord.sourceOffset()).isEqualTo(offsetManagerEntry.getProperties());
        assertThat(sourceRecord.valueSchema()).isEqualTo(Schema.BYTES_SCHEMA);
        assertThat(sourceRecord.keySchema()).isNull();
        assertThat(sourceRecord.topic()).isEqualTo(offsetManagerEntry.getTopic());
    }
}
