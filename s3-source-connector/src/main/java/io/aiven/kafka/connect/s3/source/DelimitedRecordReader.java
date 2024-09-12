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

package io.aiven.kafka.connect.s3.source;

import static java.util.Optional.ofNullable;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.DataException;

/**
 * Reads records that are followed by byte delimiters.
 */
public class DelimitedRecordReader {
    private final byte[] valueDelimiter;

    private final Optional<byte[]> keyDelimiter;

    public DelimitedRecordReader(final byte[] valueDelimiter, final Optional<byte[]> keyDelimiter) {
        this.valueDelimiter = Arrays.copyOf(valueDelimiter, valueDelimiter.length);
        this.keyDelimiter = keyDelimiter.map(delimiter -> Arrays.copyOf(delimiter, delimiter.length));
    }

    public ConsumerRecord<byte[], byte[]> read(final String topic, final int partition, final long offset,
            final BufferedInputStream data) throws IOException {
        Optional<byte[]> key = Optional.empty();
        if (keyDelimiter.isPresent()) {
            key = ofNullable(readTo(data, keyDelimiter.get()));
            if (!key.isPresent()) {
                return null;
            }
        }
        final byte[] value = readTo(data, valueDelimiter);
        if (value == null) {
            if (key.isPresent()) {
                throw new IllegalStateException("missing value for key!" + new String(key.get()));
            }
            return null;
        }
        return new ConsumerRecord<>(topic, partition, offset, key.orElse(null), value);
    }

    // read up to and including the given multi-byte delimeter
    private byte[] readTo(final BufferedInputStream data, final byte[] del) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final int lastByte = del[del.length - 1] & 0xff;
        int readCount;
        while (true) {
            readCount = data.read();
            if (readCount == -1) {
                break;
            }
            baos.write(readCount);
            if (readCount == lastByte && baos.size() >= del.length) {
                final byte[] bytes = baos.toByteArray();
                if (endsWith(bytes, del)) {
                    final byte[] undelimited = new byte[bytes.length - del.length];
                    System.arraycopy(bytes, 0, undelimited, 0, undelimited.length);
                    return undelimited;
                }
            }
        }
        // if we got here, we got EOF before we got the delimiter
        return (baos.size() == 0) ? null : baos.toByteArray();
    }

    private boolean endsWith(final byte[] bytes, final byte[] suffix) {
        for (int i = 0; i < suffix.length; i++) {
            if (bytes[bytes.length - suffix.length + i] != suffix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] delimiterBytes(final String value, final String encoding) {
        return ofNullable(value).orElse("\n")
                .getBytes(ofNullable(encoding).map(Charset::forName).orElse(StandardCharsets.UTF_8));
    }

    public static DelimitedRecordReader from(final Map<String, String> taskConfig) {
        return new DelimitedRecordReader(
                delimiterBytes(taskConfig.get("value.converter.delimiter"), taskConfig.get("value.converter.encoding")),
                taskConfig.containsKey("key.converter")
                        ? Optional.of(delimiterBytes(taskConfig.get("key.converter.delimiter"),
                                taskConfig.get("key.converter.encoding")))
                        : Optional.empty());
    }

    Iterator<ConsumerRecord<byte[], byte[]>> readAll(final String topic, final int partition,
            final InputStream inputStream, final long startOffset) {
        return new Iterator<ConsumerRecord<byte[], byte[]>>() {
            ConsumerRecord<byte[], byte[]> nextConsumerRecord;

            final BufferedInputStream buffered = new BufferedInputStream(inputStream);

            long offset = startOffset;

            @Override
            public boolean hasNext() {
                try {
                    if (nextConsumerRecord == null) {
                        nextConsumerRecord = read(topic, partition, offset++, buffered);
                    }
                } catch (IOException e) {
                    throw new DataException(e);
                }
                return nextConsumerRecord != null;
            }

            @Override
            public ConsumerRecord<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final ConsumerRecord<byte[], byte[]> record = this.nextConsumerRecord;
                nextConsumerRecord = null;
                return record;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
