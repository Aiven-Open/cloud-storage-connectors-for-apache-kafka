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

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.CommonConfig;

import io.confluent.connect.avro.AvroData;
import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Abstract test class to verify that streaming data is closed properly.
 */
class TransformerStreamingTest {

    @ParameterizedTest
    @MethodSource("testData")
    void verifyExceptionDuringIOOpen(final Transformer<?> transformer, final byte[] testData,
            final AbstractConfig config, final int expectedCount) throws IOException {
        final IOSupplier<InputStream> ioSupplier = mock(IOSupplier.class);
        when(ioSupplier.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<?> objStream = transformer.getRecords(ioSupplier, "topic", 1, config, 0);
        assertThat(objStream).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyCloseCalledAtEnd(final Transformer<?> transformer, final byte[] testData, final AbstractConfig config,
            final int expectedCount) throws IOException {
        final CloseTrackingStream stream = new CloseTrackingStream(new ByteArrayInputStream(testData));
        final Stream<?> objStream = transformer.getRecords(() -> stream, "topic", 1, config, 0);
        final long count = objStream.count();
        assertThat(count).isEqualTo(expectedCount);
        assertThat(stream.closeCount).isGreaterThan(0);
    }

    @ParameterizedTest
    @MethodSource("testData")
    void verifyCloseCalledAtIteratorEnd(final Transformer<?> transformer, final byte[] testData,
            final AbstractConfig config, final int expectedCount) throws IOException {
        final CloseTrackingStream stream = new CloseTrackingStream(new ByteArrayInputStream(testData));
        final Stream<?> objStream = transformer.getRecords(() -> stream, "topic", 1, config, 0);
        final Iterator<?> iter = objStream.iterator();
        long count = 0L;
        while (iter.hasNext()) {
            count += 1;
            iter.next();
        }
        assertThat(count).isEqualTo(expectedCount);
        assertThat(stream.closeCount).isGreaterThan(0);
    }

    static Stream<Arguments> testData() throws IOException {
        final List<Arguments> lst = new ArrayList<>();
        final AvroData avroData = new AvroData(100);
        lst.add(Arguments.of(new AvroTransformer(avroData), AvroTransformerTest.generateMockAvroData(100).toByteArray(),
                new CommonConfig(new ConfigDef(), new HashMap<>()) {
                }, 100));
        lst.add(Arguments.of(new ByteArrayTransformer(), "Hello World".getBytes(StandardCharsets.UTF_8),
                new CommonConfig(new ConfigDef(), new HashMap<>()) {
                }, 1));
        lst.add(Arguments.of(new JsonTransformer(new JsonConverter()),
                JsonTransformerTest.getJsonRecs(100).getBytes(StandardCharsets.UTF_8),
                new CommonConfig(new ConfigDef(), new HashMap<>()) {
                }, 100));
        lst.add(Arguments.of(new ParquetTransformer(avroData), ParquetTransformerTest.generateMockParquetData(),
                new CommonConfig(new ConfigDef(), new HashMap<>()) {
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
