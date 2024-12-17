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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import io.aiven.kafka.connect.common.ClosableIterator;
import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class SourceRecordIteratorTest {

    private S3SourceConfig mockConfig;
    private OffsetManager<S3OffsetManagerEntry> offsetManager;
    private Transformer transformer;

    private AWSV2SourceClient mockSourceApiClient;

    private OffsetStorageReader offsetStorageReader;

    @BeforeEach
    public void setUp() {
        mockConfig = mock(S3SourceConfig.class);
        when(mockConfig.getAwsS3BucketName()).thenReturn("BUCKET");

        offsetStorageReader = mock(OffsetStorageReader.class);
        SourceTaskContext taskContext = mock(SourceTaskContext.class);
        when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        offsetManager = new OffsetManager(taskContext);
        transformer = new TestingTransformer();
        mockSourceApiClient = mock(AWSV2SourceClient.class);

    }

    @Test
    void testIteratorProcessesS3Objects() throws IOException {

        final String key = "topic-00001-abc123.txt";

        // Mock S3Object and InputStream
        try (S3Object mockS3Object = mock(S3Object.class)) {

            when(mockSourceApiClient.getObject(anyString())).thenReturn(mockS3Object);
            when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream("This is a test".getBytes(StandardCharsets.UTF_8)), null));
            when(offsetStorageReader.offset(any())).thenReturn(null);
            when(mockSourceApiClient.getIteratorOfObjects(any())).thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));

            SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer,
                    mockSourceApiClient);

            assertThat(iterator).isExhausted();

            S3Object result = new S3Object();
            result.setKey(key);

            when(mockSourceApiClient.getIteratorOfObjects(any())).thenReturn(Collections.singletonList(result).listIterator());

            iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer, mockSourceApiClient);

            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isNotNull();
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws IOException {

        final String key = "topic-00001-abc123.txt";
        transformer = new ByteArrayTransformer();

        try (S3Object mockS3Object = mock(S3Object.class)) {

            when(mockSourceApiClient.getObject(anyString())).thenReturn(mockS3Object);
            when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(new ByteArrayInputStream("This is a test".getBytes(StandardCharsets.UTF_8)), null));
            when(mockSourceApiClient.getIteratorOfObjects(any())).thenReturn(Collections.emptyIterator());
            S3OffsetManagerEntry entry = new S3OffsetManagerEntry("BUCKET", key, "topic", 1);
            entry.incrementRecordCount();
            when(offsetStorageReader.offset(any())).thenReturn(entry.getProperties());
            S3Object s3Object = new S3Object();
            s3Object.setKey(key);
            when(mockSourceApiClient.getIteratorOfObjects(any())).thenReturn(Collections.singletonList(s3Object).listIterator());

            SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer, mockSourceApiClient);

            assertThat(iterator).isExhausted();
        }
    }

    private class TestingTransformer extends Transformer {

        private final Logger LOGGER = LoggerFactory.getLogger(TestingTransformer.class);
        @Override
        public void configureValueConverter(Map<String, String> config, AbstractConfig sourceConfig) {

        }

        @Override
        public Schema getKeySchema() {
            return null;
        }

        @Override
        protected StreamSpliterator createSpliterator(IOSupplier<InputStream> inputStreamIOSupplier, String topic, int topicPartition, AbstractConfig sourceConfig) {

                return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
                    @Override
                    protected InputStream inputOpened(final InputStream input) {
                        return input;
                    }

                    @Override
                    protected void doClose() {
                        // nothing to do.
                    }

                    @Override
                    protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {

                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            IOUtils.copy(inputStream, baos);
                            String result = "Transformed: " + baos;
                            action.accept(new SchemaAndValue(null, result.getBytes(StandardCharsets.UTF_8)));
                            return true;
                        } catch (IOException e) {
                            LOGGER.error("Error trying to advance inputStream: {}", e.getMessage(), e);
                            return false;
                        }
                    }
                };
        }
    }
}
