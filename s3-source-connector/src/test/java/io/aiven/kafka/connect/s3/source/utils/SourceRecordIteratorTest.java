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

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.RECORD_COUNT;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.BYTES_TRANSFORMATION_NUM_OF_RECS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.HashDistributionStrategy;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

final class SourceRecordIteratorTest {

    private S3OffsetManagerEntry mockS3OffsetManagerEntry;
    private OffsetManager<S3OffsetManagerEntry> mockOffsetManager;
    private Transformer mockTransformer;

    private S3Client mockS3Client;

    private AWSV2SourceClient sourceApiClient;

    @BeforeEach
    public void setUp() {
        mockOffsetManager = mock(OffsetManager.class);
        mockS3OffsetManagerEntry = mock(S3OffsetManagerEntry.class);
        mockTransformer = mock(Transformer.class);
        mockS3Client = mock(S3Client.class);
    }

    private S3SourceConfig getConfig(Map<String, String> data) {
        Map<String, String> defaults = new HashMap<>();
        defaults.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
        defaults.putAll(data);
        return new S3SourceConfig(defaults);
    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {

        final String key = "topic-00001-abc123.txt";
        final String filePattern = "{{topic}}-{{partition}}";


        S3SourceConfig config = getConfig(Collections.emptyMap());

        S3ClientBuilder builder = new S3ClientBuilder();

        sourceApiClient = new AWSV2SourceClient(builder.build(), config );


            mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);

            when(mockS3OffsetManagerEntry.getProperties()).thenReturn(Collections.emptyMap());

            Iterator<S3SourceRecord> iterator = new SourceRecordIterator(config, mockOffsetManager, mockTransformer,
                    sourceApiClient, new HashDistributionStrategy(1),
                    filePattern, 0);

            assertThat(iterator).isExhausted();

            builder.reset().addObject(key, "Hello World").endOfBlock();

        sourceApiClient = new AWSV2SourceClient(builder.build(), config );

            Iterator<S3SourceRecord> s3ObjectIterator = new SourceRecordIterator(config, mockOffsetManager, mockTransformer, sourceApiClient,
                    new HashDistributionStrategy(1), filePattern, 0);

            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isNotNull();
            assertThat(iterator).isExhausted();

    }

    @Test
    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {
        final String key = "topic-00001-abc123.txt";
        final  String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";
        final S3Object s3Object = S3Object.builder().key(key).build();

        S3SourceConfig config = getConfig(Collections.emptyMap());
        // With ByteArrayTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(sourceApiClient.getObject(key)).thenReturn(() -> inputStream);


            when(sourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());

            mockTransformer = mock(ByteArrayTransformer.class);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Stream.of(SchemaAndValue.NULL));

            when(mockOffsetManager.getEntry(any(), any())).thenReturn(Optional.of(mockS3OffsetManagerEntry));

            when(sourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());
            when(mockS3OffsetManagerEntry.getRecordCount()).thenReturn(BYTES_TRANSFORMATION_NUM_OF_RECS);

            // should skip if any records were produced by source record iterator.
            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(config, mockOffsetManager,
                    mockTransformer, sourceApiClient, new HashDistributionStrategy(1), filePattern, 0);

            assertThat(iterator).isExhausted();
            verify(sourceApiClient, never()).getObject(any());
            verify(mockTransformer, never()).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }

        // With AvroTransformer
        try (InputStream inputStream = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8))) {
            when(sourceApiClient.getObject(key)).thenReturn(() -> inputStream);

            when(sourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(s3Object).iterator());
            when(sourceApiClient.getListOfObjectKeys(any()))
                    .thenReturn(Collections.singletonList(key).listIterator());

            final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
            when(offsetStorageReader.offset(any(Map.class))).thenReturn(Map.of(RECORD_COUNT, 1));

            final SourceTaskContext context = mock(SourceTaskContext.class);
            when(context.offsetStorageReader()).thenReturn(offsetStorageReader);

            mockOffsetManager = new OffsetManager(context);

            mockTransformer = mock(Transformer.class);
            final SchemaAndValue schemaKey = new SchemaAndValue(null, "KEY");
            final SchemaAndValue schemaValue = new SchemaAndValue(null, "VALUE");
            when(mockTransformer.getKeyData(anyString(), anyString(), any())).thenReturn(schemaKey);
            when(mockTransformer.getRecords(any(), anyString(), anyInt(), any(), anyLong()))
                    .thenReturn(Arrays.asList(schemaValue).stream());

            final Iterator<S3SourceRecord> iterator = new SourceRecordIterator(config, mockOffsetManager,
                    mockTransformer, sourceApiClient, new HashDistributionStrategy(1), filePattern, 0);
            assertThat(iterator.hasNext()).isTrue();
            final S3SourceRecord record = iterator.next();
            assertThat(record.getValue().value()).isEqualTo("VALUE");
            assertThat(record.getOffsetManagerEntry().getRecordCount()).isEqualTo(2);
            verify(mockTransformer, times(1)).getRecords(any(), anyString(), anyInt(), any(), anyLong());
        }
    }
    @ParameterizedTest
    @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
                                                                            final String objectKey) {

        final  String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";
        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        S3SourceConfig config = getConfig(Collections.emptyMap());

        final S3Object obj = S3Object.builder().key(objectKey).build();

        final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
        final DistributionStrategy distributionStrategy = new HashDistributionStrategy(maxTasks) ;

        when(sourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
        when(sourceApiClient.getObject(any())).thenReturn(() -> bais);
        final SourceRecordIterator iterator = new SourceRecordIterator(config, mockOffsetManager, mockTransformer,
                sourceApiClient, distributionStrategy, filePattern, taskId);
        SourceRecordIterator.FileMatching fileMatching =  iterator.new FileMatching(filePattern);
        Predicate<S3Object> s3ObjectPredicate = fileMatching;
        s3ObjectPredicate = s3ObjectPredicate.and(s3Object -> distributionStrategy.isPartOfTask(taskId, s3Object.key(), fileMatching.pattern));
        // Assert
        assertThat(s3ObjectPredicate).accepts(obj);
    }

    @ParameterizedTest
    @CsvSource({ "4, 1, topic1-2-0", "4, 3, key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1, key3",
            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
                                                                              final String objectKey) {
        final  String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";
        mockTransformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        S3SourceConfig config = getConfig(Collections.emptyMap());

        final S3Object obj = S3Object.builder().key(objectKey).build();

        final ByteArrayInputStream bais = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
        final DistributionStrategy distributionStrategy = new HashDistributionStrategy(maxTasks) ;

        when(sourceApiClient.getS3ObjectIterator(any())).thenReturn(Arrays.asList(obj).iterator());
        when(sourceApiClient.getObject(any())).thenReturn(() -> bais);
        final SourceRecordIterator iterator = new SourceRecordIterator(config, mockOffsetManager, mockTransformer,
                sourceApiClient,distributionStrategy, filePattern, taskId);
        SourceRecordIterator.FileMatching fileMatching =  iterator.new FileMatching(filePattern);
        Predicate<S3Object> s3ObjectPredicate = fileMatching;
        s3ObjectPredicate = s3ObjectPredicate.and(s3Object -> distributionStrategy.isPartOfTask(taskId, s3Object.key(), fileMatching.pattern));
        // Assert
        assertThat(s3ObjectPredicate.test(obj)).as("Predicate should accept the objectKey: " + objectKey).isFalse();
    }

    @Test
    public void x() {
        S3ClientBuilder builder = new S3ClientBuilder();
        builder.addObject("Key", "value");
        S3Client client = builder.build();
        ListObjectsV2Response response = client.listObjectsV2(ListObjectsV2Request.builder().build());
        assertThat(response.contents()).isNotEmpty();

        sourceApiClient = new AWSV2SourceClient(builder.build(), getConfig(Collections.emptyMap()));
        Iterator<S3Object> iter = sourceApiClient.getS3ObjectIterator(null);
        assertThat(iter.hasNext());
    }


    class S3ClientBuilder  {
        Queue<Pair<List<S3Object>, Map<String, byte[]>>> blocks = new LinkedList<>();
        List<S3Object> objects = new ArrayList<>();
        Map<String, byte[]> data = new HashMap<>();

        public S3ClientBuilder addObject(String key, byte[] data) {
            objects.add(S3Object.builder().key(key).size((long)data.length).build());
            this.data.put(key, data);
            return this;
        }

        public S3ClientBuilder endOfBlock() {
            blocks.add(Pair.of(objects, data));
            return reset();
        }

        public S3ClientBuilder reset() {
            objects = new ArrayList<>();
            data = new HashMap<>();
            return this;
        }

        public S3ClientBuilder addObject(String key, String data) {
            return addObject(key, data.getBytes(StandardCharsets.UTF_8));
        }

        private ResponseBytes getResponse(String key) {
            return ResponseBytes.fromByteArray(new byte[0],data.get(key));
        }

        private ListObjectsV2Response dequeueData() {
            if (blocks.isEmpty()) {
                objects = Collections.emptyList();
                data = Collections.emptyMap();
            } else {
                Pair<List<S3Object>, Map<String, byte[]>> pair = blocks.remove();
                objects = pair.getLeft();
                data = pair.getRight();
            }
            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
        }

        public S3Client build() {
            if (!objects.isEmpty()) {
                endOfBlock();
            }
            S3Client result = mock(S3Client.class);
            when(result.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
            when(result.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
            when(result.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
            return result;
        }

    }
}
