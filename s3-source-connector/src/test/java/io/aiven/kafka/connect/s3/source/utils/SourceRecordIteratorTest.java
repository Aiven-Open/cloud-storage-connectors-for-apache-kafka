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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.ClosableIterator;
import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
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
        final SourceTaskContext taskContext = mock(SourceTaskContext.class);
        when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        offsetManager = new OffsetManager<>(taskContext);
        transformer = new TestingTransformer();
        mockSourceApiClient = mock(AWSV2SourceClient.class);

    }

    @Test
    void testIteratorProcessesS3Objects() {

        final String key = "topic-00001-abc123.txt";

        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(mockSourceApiClient.getIteratorOfObjects(any()))
                .thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));

        SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer,
                mockSourceApiClient);

        assertThat(iterator).isExhausted();

        final S3Object result = new S3Object(); // NOPMD closed during testing below.
        result.setKey(key);
        result.setObjectContent(new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8)));

        when(mockSourceApiClient.getIteratorOfObjects(any()))
                .thenReturn(Collections.singletonList(result).listIterator())
                .thenReturn(Collections.emptyIterator());

        iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer, mockSourceApiClient);

        assertThat(iterator).hasNext();
        final S3SourceRecord sourceRecord = iterator.next();
        assertThat(sourceRecord).isNotNull();
        assertThat(sourceRecord.value().value()).isEqualTo("Transformed: Hello World");
        assertThat(sourceRecord.getObjectKey()).isEqualTo(key);
        assertThat(sourceRecord.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
        assertThat(iterator).isExhausted();
    }

    @Test
    void testIteratorProcessesS3ObjectsForByteArrayTransformer() throws IOException {

        final String key = "topic-00001-abc123.txt";
        transformer = new ByteArrayTransformer();

        try (S3Object mockS3Object = mock(S3Object.class)) {
            when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(
                    new ByteArrayInputStream("This is a test".getBytes(StandardCharsets.UTF_8)), null));
            when(mockSourceApiClient.getIteratorOfObjects(any())).thenReturn(Collections.emptyIterator());
            final S3OffsetManagerEntry entry = new S3OffsetManagerEntry("BUCKET", key, "topic", 1);
            entry.incrementRecordCount();
            when(offsetStorageReader.offset(any())).thenReturn(entry.getProperties());
            final S3Object s3Object = new S3Object(); // NOPMD object closed below
            s3Object.setKey(key);
            when(mockSourceApiClient.getIteratorOfObjects(any()))
                    .thenReturn(Collections.singletonList(s3Object).listIterator());

            final SourceRecordIterator iterator = new SourceRecordIterator(mockConfig, offsetManager, transformer,
                    mockSourceApiClient);
            assertThat(iterator).isExhausted();
        }
    }

    @SuppressWarnings("PMD.TestClassWithoutTestCases") // TODO figure out why this fails.
    private static class TestingTransformer extends Transformer { // NOPMD because the above supress warnings does not
                                                                  // work.
        private final static Logger LOGGER = LoggerFactory.getLogger(TestingTransformer.class);

        @Override
        public Schema getKeySchema() {
            return null;
        }

        @Override
        protected StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
                final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, final SourceCommonConfig sourceConfig) {

            return new StreamSpliterator(LOGGER, inputStreamIOSupplier, offsetManagerEntry) {
                private boolean wasRead;
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
                    if (wasRead) {
                        return false;
                    }
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        IOUtils.copy(inputStream, baos);
                        final String result = "Transformed: " + baos;
                        action.accept(new SchemaAndValue(null, result));
                        wasRead = true;
                        return true;
                    } catch (RuntimeException | IOException e) { // NOPMD must catch runtime exception here.
                        LOGGER.error("Error trying to advance inputStream: {}", e.getMessage(), e);
                        wasRead = true;
                        return false;
                    }
                }
            };
        }
    }
}
