/*
 * Copyright 2021 Aiven Oy
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public final class ParquetUtils {

    public static final String TMP_DIR = "/tmp";
    public static final int BUFFER_SIZE = 8192;

    private ParquetUtils() {
        /* hide constructor */ }

    public static List<GenericRecord> getRecords(final InputStream inputStream, final String topic) throws IOException {
        final Path tmpDir = Paths.get(TMP_DIR);

        final String timestamp = String.valueOf(Instant.now().toEpochMilli());
        final Path parquetFile = tmpDir.resolve(topic + "_" + timestamp + ".parquet");

        // Write the byte array to a file
        try (OutputStream outputStream = Files.newOutputStream(parquetFile)) {
            final byte[] buffer = new byte[BUFFER_SIZE];

            int bytesRead = inputStream.read(buffer);
            while (bytesRead != -1) {
                outputStream.write(buffer, 0, bytesRead); // Write buffer to file
                bytesRead = inputStream.read(buffer);
            }
        } catch (IOException e) {
            throw new ConnectException("Error writing tmp parquet file", e);
        }

        final var records = new ArrayList<GenericRecord>();
        final var seekableByteChannel = Files.newByteChannel(parquetFile);
        try (var parquetReader = AvroParquetReader.<GenericRecord>builder(new InputFile() {
            @Override
            public long getLength() throws IOException {
                return seekableByteChannel.size();
            }

            @Override
            public SeekableInputStream newStream() {
                return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {
                    @Override
                    public long getPos() throws IOException {
                        return seekableByteChannel.position();
                    }

                    @Override
                    public void seek(final long value) throws IOException {
                        seekableByteChannel.position(value);
                    }
                };
            }

        }).withCompatibility(false).build()) {
            var record = parquetReader.read();
            while (record != null) {
                records.add(record);
                record = parquetReader.read();
            }
        }
        if (Files.exists(parquetFile)) {
            Files.delete(parquetFile);
        }

        return records;
    }

}
