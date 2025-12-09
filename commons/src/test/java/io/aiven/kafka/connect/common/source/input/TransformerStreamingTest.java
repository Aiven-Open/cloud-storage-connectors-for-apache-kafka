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

package io.aiven.kafka.connect.common.source.input;

import static io.aiven.kafka.connect.common.source.input.Transformer.UNKNOWN_STREAM_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify that streaming data passing through a transformer is closed properly.
 */
class TransformerStreamingTest {

    private Context<String> context;

    @BeforeEach
    public void setup() {
        context = new Context<>("storage-key");
        context.setTopic("topic");
        context.setPartition(1);
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyExceptionDuringIOOpen(final Transformer transformer, final byte[] testData,
            final SourceCommonConfig config, final int expectedCount) throws IOException {
        final IOSupplier<InputStream> ioSupplier = mock(IOSupplier.class);
        when(ioSupplier.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<?> objStream = transformer.getRecords(ioSupplier, UNKNOWN_STREAM_LENGTH, context, config, 0);
        assertThat(objStream).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyExceptionDuringRead(final Transformer transformer, final byte[] testData,
            final SourceCommonConfig config, final int expectedCount) throws IOException {
        try (InputStream inputStream = mock(InputStream.class)) {
            when(inputStream.read()).thenThrow(new IOException("Test IOException during read"));
            when(inputStream.read(any())).thenThrow(new IOException("Test IOException during read"));
            when(inputStream.read(any(), anyInt(), anyInt()))
                    .thenThrow(new IOException("Test IOException during read"));
            when(inputStream.readNBytes(any(), anyInt(), anyInt()))
                    .thenThrow(new IOException("Test IOException during read"));
            when(inputStream.readNBytes(anyInt())).thenThrow(new IOException("Test IOException during read"));
            when(inputStream.readAllBytes()).thenThrow(new IOException("Test IOException during read"));
            try (CloseTrackingStream stream = new CloseTrackingStream(inputStream)) {
                final Stream<?> objStream = transformer.getRecords(() -> stream, UNKNOWN_STREAM_LENGTH, context, config,
                        0);
                assertThat(objStream).isEmpty();
                assertThat(stream.closeCount).isGreaterThan(0);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyCloseCalledAtEnd(final Transformer transformer, final byte[] testData, final SourceCommonConfig config,
            final int expectedCount) throws IOException {
        final CloseTrackingStream stream = new CloseTrackingStream(new ByteArrayInputStream(testData));
        final Stream<?> objStream = transformer.getRecords(() -> stream, UNKNOWN_STREAM_LENGTH, context, config, 0);
        final long count = objStream.count();
        assertThat(count).isEqualTo(expectedCount);
        assertThat(stream.closeCount).isGreaterThan(0);
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyCloseCalledAtIteratorEnd(final Transformer transformer, final byte[] testData,
            final SourceCommonConfig config, final int expectedCount) throws IOException {
        final CloseTrackingStream stream = new CloseTrackingStream(new ByteArrayInputStream(testData));
        final Stream<SchemaAndValue> objStream = transformer.getRecords(() -> stream, UNKNOWN_STREAM_LENGTH, context,
                config, 0);
        final Iterator<SchemaAndValue> iter = objStream.iterator();
        long count = 0L;
        while (iter.hasNext()) {
            count += 1;
            iter.next();
        }
        assertThat(count).isEqualTo(expectedCount);
        assertThat(stream.closeCount).isGreaterThan(0);
    }

    static SourceCommonConfig.SourceCommonConfigDef baseConfigDef() {
        return new SourceCommonConfig.SourceCommonConfigDef();
    }

    static Stream<Arguments> testData() throws IOException {
        final List<Arguments> lst = new ArrayList<>();
        final Map<String, String> props = new HashMap<>();
        CommonConfigFragment.setter(props).name("testing name").connector(Connector.class);
        OutputFormatFragment.setter(props).withFormatType(FormatType.AVRO);
        FileNameFragment.setter(props).template(".*");

        lst.add(Arguments.of(TransformerFactory.getTransformer(InputFormat.AVRO),
                AvroTransformerTest.generateMockAvroData(100).toByteArray(),
                new SourceCommonConfig(baseConfigDef(), props) {
                }, 100));

        final SourceCommonConfig.SourceCommonConfigDef configDef = baseConfigDef();

        lst.add(Arguments.of(TransformerFactory.getTransformer(InputFormat.BYTES),
                "Hello World".getBytes(StandardCharsets.UTF_8), new SourceCommonConfig(configDef, props) {
                }, 1));

        lst.add(Arguments.of(TransformerFactory.getTransformer(InputFormat.JSONL),
                JsonTransformerTest.getJsonRecs(100).getBytes(StandardCharsets.UTF_8),
                new SourceCommonConfig(baseConfigDef(), props) {
                }, 100));

        lst.add(Arguments.of(TransformerFactory.getTransformer(InputFormat.PARQUET),
                ParquetTransformerTest.generateMockParquetData(), new SourceCommonConfig(baseConfigDef(), props) {
                }, 100));
        return lst.stream();
    }

    private static class CloseTrackingStream extends InputStream {
        InputStream delegate;
        int closeCount;

        CloseTrackingStream(final InputStream stream) {
            super();
            this.delegate = stream;
        }

        @Override
        public int read() throws IOException {
            if (closeCount > 0) {
                throw new IOException("ERROR Read after close");
            }
            return delegate.read();
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            delegate.close();
        }
    }
}
