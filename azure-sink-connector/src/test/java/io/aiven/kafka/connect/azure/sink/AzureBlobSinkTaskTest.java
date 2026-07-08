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

package io.aiven.kafka.connect.azure.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import io.aiven.kafka.connect.azure.sink.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.common.config.CompressionType;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class AzureBlobSinkTaskTest {
    private static final String TEST_CONTAINER_NAME = "test-container";

    @Mock
    private BlobServiceClient blobServiceClient;
    @Mock
    private BlobContainerClient blobContainerClient;
    @Mock
    private BlobClient blobClient;
    @Mock
    private BlockBlobClient blockBlobClient;
    @Mock
    private BlobOutputStream blobOutputStream;
    @Mock
    private PagedIterable<BlobItem> pagedIterable;
    @Mock
    private SinkTaskContext taskContext;
    private AzureBlobSinkTask task;
    private Map<String, String> properties;
    private AzureBlobAccessor testBlobAccessor;

    @BeforeEach
    void setUp() {
        // Configure the mocks (lenient: not every test triggers a write, so some stubs stay unused).
        lenient().when(blobServiceClient.getBlobContainerClient(anyString())).thenReturn(blobContainerClient);
        lenient().when(blobContainerClient.getBlobClient(anyString())).thenReturn(blobClient);
        lenient().when(blobClient.getBlockBlobClient()).thenReturn(blockBlobClient);
        lenient().when(blockBlobClient.getBlobOutputStream(anyBoolean())).thenReturn(blobOutputStream);
        lenient().when(taskContext.assignment()).thenReturn(Collections.emptySet());

        // Initialize properties
        properties = new HashMap<>();
        properties.put("azure.storage.connection.string", "UseDevelopmentStorage=true");
        properties.put("azure.storage.container.name", TEST_CONTAINER_NAME);
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}");
        properties.put("format.output.fields", "value");

        // Initialize the task with the mocked dependencies
        task = new AzureBlobSinkTask(properties, blobServiceClient);
        task.initialize(taskContext);

        // Initialize the AzureBlobAccessor
        testBlobAccessor = new AzureBlobAccessor(blobContainerClient, false);
    }

    @Test
    void testPutAndFlush() throws Exception {
        // Create a mock SinkRecord
        final SinkRecord record = createRecord("topic0", 0, "key0", "value0", 10, System.currentTimeMillis());
        task.put(Collections.singletonList(record));

        // Perform the flush operation
        task.flush(null);

        // Verify interactions
        verify(blobOutputStream, times(1)).write(any(byte[].class), anyInt(), anyInt());
        verify(blobOutputStream, times(1)).close();
    }

    @Test
    void noWriteWhenUnderThresholds() {
        // Buffered size < AZURE_WRITE_BUFFER_SIZE_BYTES_PER_TASK AND time < AZURE_WRITE_INTERVAL_MS -> no write.
        task.put(Collections.singletonList(createRecord("topic0", 0, "key0", "value0", 0, 1000)));
        verify(blockBlobClient, never()).getBlobOutputStream(anyBoolean());

        // The remaining buffered records are only written out on flush().
        task.flush(null);
        verify(blockBlobClient, times(1)).getBlobOutputStream(anyBoolean());
    }

    @Test
    void writeWhenTimeThresholdExceeded() throws IOException {
        // Time interval > AZURE_WRITE_INTERVAL_MS -> trigger write during put().
        final MutableClock testClock = new MutableClock(Instant.now());
        task = new AzureBlobSinkTask(properties, blobServiceClient, testClock);
        task.initialize(taskContext);

        task.put(Collections.singletonList(createRecord("topic0", 0, "key0", "value0", 0, 1000)));
        verify(blockBlobClient, never()).getBlobOutputStream(anyBoolean()); // buffered, not written

        testClock.advance(Duration.ofSeconds(11));

        task.put(Collections.singletonList(createRecord("topic0", 0, "key1", "value1", 1, 1001)));
        verify(blockBlobClient, times(1)).getBlobOutputStream(anyBoolean()); // written during put()

        task.flush(null);
        verify(blobOutputStream, times(1)).close();
    }

    @Test
    void shouldRequestCommitWhenMaxRecordsPerFileExceeded() {
        properties.put(AzureBlobSinkConfig.FILE_MAX_RECORDS, "2");
        task = new AzureBlobSinkTask(properties, blobServiceClient);
        task.initialize(taskContext);

        task.put(Collections.singletonList(createRecord("topic0", 0, "key0", "val0", 10, 1000)));
        verify(taskContext, never()).requestCommit();

        task.put(Collections.singletonList(createRecord("topic0", 0, "key1", "val1", 11, 1001)));

        // Verify the signal was sent to the Worker.
        verify(taskContext).requestCommit();
    }

    @Test
    void shouldRequestCommitWhenMaxBytesPerFileExceeded() {
        properties.put(AzureBlobSinkConfig.FILE_MAX_BYTES, "100");
        task = new AzureBlobSinkTask(properties, blobServiceClient);
        task.initialize(taskContext);

        final byte[] largeValue = new byte[40];
        final SinkRecord record = new SinkRecord("topic0", 0, Schema.STRING_SCHEMA, "key", Schema.BYTES_SCHEMA,
                largeValue, 10, 1000L, TimestampType.CREATE_TIME);

        task.put(Collections.singletonList(record));
        verify(taskContext, never()).requestCommit();

        task.put(Collections.singletonList(record));

        // Verify the threshold check triggered the request.
        verify(taskContext).requestCommit();
    }

    @Test
    void shouldNotRequestCommitWhenOneRecordPerFileIsActive() {
        properties.put("file.name.template", "{{key}}");
        properties.put(AzureBlobSinkConfig.FILE_MAX_RECORDS, "1");
        properties.put(AzureBlobSinkConfig.FILE_MAX_BYTES, "100");
        task = new AzureBlobSinkTask(properties, blobServiceClient);
        task.initialize(taskContext);

        final byte[] largeValue = new byte[40];
        final SinkRecord record = new SinkRecord("topic0", 0, Schema.STRING_SCHEMA, "key", Schema.BYTES_SCHEMA,
                largeValue, 10, 1000L, TimestampType.CREATE_TIME);

        task.put(Collections.singletonList(record));
        verify(taskContext, never()).requestCommit();

        task.put(Collections.singletonList(record));
        verify(taskContext, never()).requestCommit();
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void basic(final String compression) {
        final List<BlobItem> blobItems = generateTestBlobItems(compression);
        when(pagedIterable.spliterator()).thenReturn(blobItems.spliterator());
        when(blobContainerClient.listBlobs()).thenReturn(pagedIterable);
        properties.put(AzureBlobSinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        task = new AzureBlobSinkTask(properties, blobServiceClient);
        task.initialize(taskContext);

        final List<SinkRecord> records = createBasicRecords();
        task.put(records);
        task.flush(null);

        final Map<String, Collection<List<String>>> blobNameWithExtensionValuesMap = buildBlobNameValuesMap(
                compression);

        // Validate that all expected blob names are present in the actual blob names list
        assertThat(testBlobAccessor.getBlobNames()).containsAll(blobNameWithExtensionValuesMap.keySet());
    }

    private List<BlobItem> generateTestBlobItems(final String compression) {
        // Create mock BlobItems for different compression types
        final List<BlobItem> blobItems = new ArrayList<>();

        // No compression (plain)
        blobItems.add(createMockBlobItem("topic0-0-10", compression));
        blobItems.add(createMockBlobItem("topic0-1-20", compression));
        blobItems.add(createMockBlobItem("topic0-2-50", compression));
        blobItems.add(createMockBlobItem("topic1-0-30", compression));
        blobItems.add(createMockBlobItem("topic1-1-40", compression));

        return blobItems;
    }

    private BlobItem createMockBlobItem(final String name, final String compression) {
        final BlobItem blobItem = mock(BlobItem.class);
        if ("none".equals(compression)) {
            when(blobItem.getName()).thenReturn(name);
        } else if ("gzip".equals(compression)) {
            when(blobItem.getName()).thenReturn(name + ".gz");
        } else if ("zstd".equals(compression)) {
            when(blobItem.getName()).thenReturn(name + ".zst");
        } else {
            when(blobItem.getName()).thenReturn(name + "." + compression);
        }
        return blobItem;
    }

    private SinkRecord createRecord(final String topic, final int partition, final String key, final String value,
            final int offset, final long timestamp) {
        return new SinkRecord(topic, partition, Schema.BYTES_SCHEMA, key.getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA, value.getBytes(StandardCharsets.UTF_8), offset, timestamp, null, null);
    }

    private List<SinkRecord> createBasicRecords() {
        return Arrays.asList(createRecord("topic0", 0, "key0", "value0", 10, 1000),
                createRecord("topic0", 1, "key1", "value1", 20, 1001),
                createRecord("topic1", 0, "key2", "value2", 30, 1002),
                createRecord("topic1", 1, "key3", "value3", 40, 1003),
                createRecord("topic0", 2, "key4", "value4", 50, 1004),
                createRecord("topic0", 0, "key5", "value5", 11, 1005),
                createRecord("topic0", 1, "key6", "value6", 21, 1006),
                createRecord("topic1", 0, "key7", "value7", 31, 1007),
                createRecord("topic1", 1, "key8", "value8", 41, 1008),
                createRecord("topic0", 2, "key9", "value9", 51, 1009));
    }

    private Map<String, Collection<List<String>>> buildBlobNameValuesMap(final String compression) {
        final CompressionType compressionType = CompressionType.forName(compression);
        final String extension = compressionType.extension();
        final Map<String, Collection<List<String>>> blobNameValuesMap = new HashMap<>();
        blobNameValuesMap.put("topic0-0-10" + extension, toCollectionOfLists("value0", "value5"));
        blobNameValuesMap.put("topic0-1-20" + extension, toCollectionOfLists("value1", "value6"));
        blobNameValuesMap.put("topic1-0-30" + extension, toCollectionOfLists("value2", "value7"));
        blobNameValuesMap.put("topic1-1-40" + extension, toCollectionOfLists("value3", "value8"));
        blobNameValuesMap.put("topic0-2-50" + extension, toCollectionOfLists("value4", "value9"));
        return blobNameValuesMap;
    }

    private Collection<List<String>> toCollectionOfLists(final String... values) {
        return Arrays.stream(values).map(Collections::singletonList).collect(Collectors.toList());
    }

    static class MutableClock extends Clock {
        private Instant clock;
        private final ZoneId zone;

        MutableClock(final Instant start) {
            super();
            this.clock = start;
            this.zone = ZoneId.of("UTC");
        }

        void advance(final Duration duration) {
            this.clock = this.clock.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return new MutableClock(clock);
        }

        @Override
        public Instant instant() {
            return clock;
        }
    }
}
