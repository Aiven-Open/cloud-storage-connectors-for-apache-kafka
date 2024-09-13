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
                throw new IllegalStateException(
                        "missing value for key!" + new String(key.get(), StandardCharsets.UTF_8));
            }
            return null;
        }
        return new ConsumerRecord<>(topic, partition, offset, key.orElse(null), value);
    }

    // read up to and including the given multi-byte delimeter
    private byte[] readTo(final BufferedInputStream data, final byte[] del) throws IOException {
        final int lastByte = del[del.length - 1] & 0xff;
        byte[] buffer = new byte[1024]; // Buffer for reading data, adjust size as needed
        int bufferIndex = 0; // Tracks the current position in the buffer
        int readCount;

        while (true) {
            readCount = data.read();
            if (readCount == -1) {
                // Return null if no bytes were read and EOF is reached
                return (bufferIndex == 0) ? null : Arrays.copyOf(buffer, bufferIndex);
            }

            // Write the byte to the buffer
            if (bufferIndex >= buffer.length) {
                // Resize buffer if needed, avoiding frequent resizing
                buffer = Arrays.copyOf(buffer, buffer.length * 2);
            }
            buffer[bufferIndex++] = (byte) readCount;

            // Check for delimiter match
            final Optional<byte[]> optionalBytes = getBytes(del, lastByte, buffer, bufferIndex, readCount);
            if (optionalBytes.isPresent()) {
                return optionalBytes.get();
            }
        }
    }

    private static Optional<byte[]> getBytes(final byte[] del, final int lastByte, final byte[] buffer,
            final int bufferIndex, final int readCount) {
        if (readCount == lastByte && bufferIndex >= del.length) {
            boolean matches = true;
            for (int i = 0; i < del.length; i++) {
                if (buffer[bufferIndex - del.length + i] != del[i]) {
                    matches = false;
                    break;
                }
            }

            if (matches) {
                // Return undelimited data without creating new objects inside the loop
                final byte[] undelimited = new byte[bufferIndex - del.length];
                System.arraycopy(buffer, 0, undelimited, 0, undelimited.length);
                return Optional.of(undelimited);
            }
        }
        // Return Optional.empty() to signify no match was found
        return Optional.empty();
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
            Optional<ConsumerRecord<byte[], byte[]>> nextConsumerRecord;

            final BufferedInputStream buffered = new BufferedInputStream(inputStream);

            long offset = startOffset;

            @Override
            public boolean hasNext() {
                try {
                    if (nextConsumerRecord.isPresent()) {
                        nextConsumerRecord = ofNullable(read(topic, partition, offset++, buffered));
                    }
                } catch (IOException e) {
                    throw new DataException(e);
                }
                return nextConsumerRecord.isPresent();
            }

            @Override
            public ConsumerRecord<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final ConsumerRecord<byte[], byte[]> record = this.nextConsumerRecord.get();
                nextConsumerRecord = Optional.empty();
                return record;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
