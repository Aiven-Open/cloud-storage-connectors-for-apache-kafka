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

package io.aiven.kafka.connect.azure.source.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.task.Context;

import com.azure.storage.blob.models.BlobItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AzureBlobSourceRecordTest {

    public static final String TEST_BLOB_NAME_TXT = "test-blob-name.txt";
    public static final String CONTAINER = "container";
    @Mock
    private AzureBlobSourceConfig azureBlobSourceConfig;

    @Mock
    private OffsetManager<AzureOffsetManagerEntry> offsetManager;

    private Context<String> context;

    @Mock
    private BlobItem blobItem;

    @Test
    void testCreateSourceRecord() {
        final String topic = "test-topic";
        final AzureOffsetManagerEntry entry = new AzureOffsetManagerEntry(CONTAINER, TEST_BLOB_NAME_TXT);
        context = new Context<>(TEST_BLOB_NAME_TXT);
        context.setPartition(null);
        context.setTopic(topic);
        final AzureBlobSourceRecord azureBlobSourceRecord = new AzureBlobSourceRecord(blobItem);
        azureBlobSourceRecord.setOffsetManagerEntry(entry);
        azureBlobSourceRecord.setContext(context);
        azureBlobSourceRecord.setValueData(new SchemaAndValue(null, ""));
        azureBlobSourceRecord.setKeyData(new SchemaAndValue(null, ""));

        final SourceRecord result = azureBlobSourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);

        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

    }

    @Test
    void testCreateSourceRecordWithDataError() {
        context = mock(Context.class);
        final AzureOffsetManagerEntry offsetManagerEntry = mock(AzureOffsetManagerEntry.class);
        when(offsetManagerEntry.getManagerKey()).thenThrow(new DataException("Test Exception"));
        when(offsetManagerEntry.fromProperties(any())).thenReturn(offsetManagerEntry);
        final AzureBlobSourceRecord azureBlobSourceRecord = new AzureBlobSourceRecord(blobItem);
        azureBlobSourceRecord.setOffsetManagerEntry(offsetManagerEntry);
        azureBlobSourceRecord.setContext(context);

        assertThatExceptionOfType(ConnectException.class).as("Errors tolerance: NONE")
                .isThrownBy(() -> azureBlobSourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager));
        final SourceRecord result = azureBlobSourceRecord.getSourceRecord(ErrorsTolerance.ALL, offsetManager);
        assertThat(result).isNull();
    }

    @Test
    void testModifyingInitialContextDoesNotAlterTheSourceRecordsContext() {
        final String topic = "test-topic";
        final AzureOffsetManagerEntry entry = new AzureOffsetManagerEntry(CONTAINER, TEST_BLOB_NAME_TXT);
        context = new Context<>(TEST_BLOB_NAME_TXT);
        context.setPartition(null);
        context.setTopic(topic);
        final AzureBlobSourceRecord azureBlobSourceRecord = new AzureBlobSourceRecord(blobItem);
        azureBlobSourceRecord.setOffsetManagerEntry(entry);
        azureBlobSourceRecord.setContext(context);
        azureBlobSourceRecord.setValueData(new SchemaAndValue(null, ""));
        azureBlobSourceRecord.setKeyData(new SchemaAndValue(null, ""));

        // alter context and it should have no impact on the source record.
        context.setPartition(14);
        context.setTopic("a-diff-topic");
        SourceRecord result = azureBlobSourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

        // We should return a defensive copy so altering here should not affect the ssSourceRecord
        context = azureBlobSourceRecord.getContext();
        context.setPartition(99);
        context.setTopic("another-diff-topic");
        result = azureBlobSourceRecord.getSourceRecord(ErrorsTolerance.NONE, offsetManager);
        assertThat(result.topic()).isEqualTo(topic);
        assertThat(result.kafkaPartition()).isEqualTo(null);

    }

    @Test
    void testModifyingInitialOffsetManagerEntryDoesNotAlterTheSourceRecordsOffsetManagerEntry() {
        final String topic = "test-topic";
        AzureOffsetManagerEntry entry = new AzureOffsetManagerEntry(CONTAINER, TEST_BLOB_NAME_TXT);
        context = new Context<>(TEST_BLOB_NAME_TXT);
        context.setPartition(null);
        context.setTopic(topic);
        final AzureBlobSourceRecord azureSourceRecord = new AzureBlobSourceRecord(blobItem);
        azureSourceRecord.setOffsetManagerEntry(entry);
        azureSourceRecord.setContext(context);
        azureSourceRecord.setValueData(new SchemaAndValue(null, ""));
        azureSourceRecord.setKeyData(new SchemaAndValue(null, ""));
        final long currentRecordCount = entry.getRecordCount();
        // alter entry record count and it should have no impact on the source record.
        entry.incrementRecordCount();
        assertThat(azureSourceRecord.getRecordCount()).isEqualTo(currentRecordCount);

        // We should return a defensive copy so altering here should not affect the ssSourceRecord
        entry = azureSourceRecord.getOffsetManagerEntry();
        entry.incrementRecordCount();
        entry.incrementRecordCount();
        assertThat(azureSourceRecord.getRecordCount()).isEqualTo(currentRecordCount);

    }

}
