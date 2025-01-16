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
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

final class SourceRecordIteratorTest {

    private AWSV2SourceClient mockSourceApiClient;

    private OffsetStorageReader offsetStorageReader;

    private OffsetManager offsetManager;

    private S3SourceConfig mockS3SourceConfig;

//    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager,
//                                final Transformer transformer, final AWSV2SourceClient sourceClient,
//                                final DistributionStrategy distributionStrategy, final String filePattern, final int taskId)

    @BeforeEach
    void setup() {
        final SourceTaskContext mockTaskContext = mock(SourceTaskContext.class);
        offsetStorageReader = mock(OffsetStorageReader.class);
        when(mockTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        offsetManager = new OffsetManager(mockTaskContext);
        mockSourceApiClient = mock(AWSV2SourceClient.class);
        mockS3SourceConfig = mock(S3SourceConfig.class);
        when(mockS3SourceConfig.getAwsS3BucketName()).thenReturn("bucket-name");
    }

//    private S3SourceConfig getConfig(Map<String, String> data) {
//        Map<String, String> defaults = new HashMap<>();
//        defaults.put(AWS_S3_BUCKET_NAME_CONFIG, "bucket-name");
//        defaults.putAll(data);
//        return new S3SourceConfig(defaults);
//    }

    @Test
    void testIteratorProcessesS3Objects() throws Exception {

        final String key = "topic-00001-abc123.txt";
        final String filePattern = "{{topic}}-{{partition}}";
        final Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        when(offsetStorageReader.offset(any(Map.class))).thenReturn(new HashMap<>());
        when(mockSourceApiClient.getS3ObjectStream(any())).thenReturn(Stream.empty());

        Iterator<S3SourceRecord> iterator = new SourceRecordIterator(mockS3SourceConfig, offsetManager, transformer,
                mockSourceApiClient, new HashDistributionStrategy(1),
                filePattern, 0);

        assertThat(iterator).isExhausted();

        S3Object s3Object = S3Object.builder().key(key).size(1L).build();
        when(mockSourceApiClient.getS3ObjectStream(any())).thenReturn(Collections.singletonList(s3Object).stream());
        when(mockSourceApiClient.getObject(key)).thenReturn(() -> new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8)));

        iterator = new SourceRecordIterator(mockS3SourceConfig, offsetManager, transformer, mockSourceApiClient,
                new HashDistributionStrategy(1), filePattern, 0);

        assertThat(iterator).hasNext();
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();

    }

//    @Test
//    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws Exception {
//        final String key = "topic-00001-abc123.txt";
//        final String filePattern = "{{topic}}-{{partition}}";
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        final S3SourceConfig config = getConfig(Collections.emptyMap());
//
//        // With ByteArrayTransformer
//        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//
//        builder.addObject(key, "Hello World");
//
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        when(offsetStorageReader.offset(any(Map.class))).thenReturn(Map.of(RECORD_COUNT, 1));
//
//
//        // should skip if any records were produced by source record iterator.
//        Iterator<S3SourceRecord> iterator = new SourceRecordIterator(config, offsetManager,
//                transformer, sourceApiClient, new HashDistributionStrategy(1), filePattern, 0);
//
//        assertThat(iterator).isExhausted();
//
//
//        // With JsonTransformer
//        StringBuilder jsonContent = new StringBuilder();
//        for (int i = 0; i < 5; i++) {
//            jsonContent.append(String.format("{\"message\": \"Hello World\", \"id\":\"%s\"}%n", i));
//        }
//
//        builder.reset().addObject(key, jsonContent.toString());
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        transformer = TransformerFactory.getTransformer(InputFormat.JSONL);
//
//        iterator = new SourceRecordIterator(config, offsetManager,
//                transformer, sourceApiClient, new HashDistributionStrategy(1), filePattern, 0);
//        assertThat(iterator).hasNext();
//        final S3SourceRecord record = iterator.next();
//        assertThat((Map) record.getValue().value()).containsEntry("id", "1");
//    }
//
//    @ParameterizedTest
//    @ValueSource( ints={0, 1, 2, 3})
//    void testRetrieveTaskBasedData(int taskId) {
//        Map<Integer, String> expectedTask = Map.of(3, "key-1", 0, "key-2", 1, "key-3", 2, "key-4");
//
//        int maxTasks = 4;
//        final  String filePattern = "{{topic}}-{{partition}}";
//        final S3SourceConfig config = getConfig(Collections.emptyMap());
//        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
//
//        final S3ClientBuilder builder = new S3ClientBuilder();
//        for (String key : expectedTask.values()) {
//            builder.addObject(key, "Hello World - "+key);
//        }
//        sourceApiClient = new AWSV2SourceClient(builder.build(), config);
//
//        final DistributionStrategy distributionStrategy = new HashDistributionStrategy(maxTasks) ;
//
//        final SourceRecordIterator iterator = new SourceRecordIterator(config, offsetManager, transformer,
//                sourceApiClient, distributionStrategy, filePattern, taskId);
//
//        assertThat(iterator.hasNext());
//        S3SourceRecord record = iterator.next();
//        assertThat(record.getObjectKey()).isEqualTo(expectedTask.get(taskId));
//        assertThat(iterator).isExhausted();
//    }
//
//    class S3ClientBuilder  {
//        Queue<Pair<List<S3Object>, Map<String, byte[]>>> blocks = new LinkedList<>();
//        List<S3Object> objects = new ArrayList<>();
//        Map<String, byte[]> data = new HashMap<>();
//
//        public S3ClientBuilder addObject(String key, byte[] data) {
//            objects.add(S3Object.builder().key(key).size((long)data.length).build());
//            this.data.put(key, data);
//            return this;
//        }
//
//        public S3ClientBuilder endOfBlock() {
//            blocks.add(Pair.of(objects, data));
//            return reset();
//        }
//
//        public S3ClientBuilder reset() {
//            objects = new ArrayList<>();
//            data = new HashMap<>();
//            return this;
//        }
//
//        public S3ClientBuilder addObject(String key, String data) {
//            return addObject(key, data.getBytes(StandardCharsets.UTF_8));
//        }
//
//        private ResponseBytes getResponse(String key) {
//            return ResponseBytes.fromByteArray(new byte[0],data.get(key));
//        }
//
//        private ListObjectsV2Response dequeueData() {
//            if (blocks.isEmpty()) {
//                objects = Collections.emptyList();
//                data = Collections.emptyMap();
//            } else {
//                Pair<List<S3Object>, Map<String, byte[]>> pair = blocks.remove();
//                objects = pair.getLeft();
//                data = pair.getRight();
//            }
//            return ListObjectsV2Response.builder().contents(objects).isTruncated(false).build();
//        }
//
//        public S3Client build() {
//            if (!objects.isEmpty()) {
//                endOfBlock();
//            }
//            S3Client result = mock(S3Client.class);
//            when(result.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(env -> dequeueData());
//            when(result.listObjectsV2(any(Consumer.class))).thenAnswer(env -> dequeueData());
//            when(result.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(env -> getResponse(env.getArgument(0, GetObjectRequest.class).key()));
//            return result;
//        }
//
//    }
}
