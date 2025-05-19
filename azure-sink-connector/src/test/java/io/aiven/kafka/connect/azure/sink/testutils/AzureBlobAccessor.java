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

package io.aiven.kafka.connect.azure.sink.testutils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

import io.aiven.kafka.connect.common.config.CompressionType;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;

public final class AzureBlobAccessor {
    private final BlobContainerClient containerClient;
    private final boolean cache;

    private List<String> blobNamesCache;
    private final Map<String, String> stringContentCache = new HashMap<>();
    private final Map<String, List<String>> linesCache = new HashMap<>();
    private final Map<String, List<List<String>>> decodedLinesCache = new HashMap<>();

    public AzureBlobAccessor(final BlobContainerClient containerClient, final boolean cache) {
        Objects.requireNonNull(containerClient, "containerClient cannot be null");

        this.containerClient = containerClient;
        this.cache = cache;
    }

    public AzureBlobAccessor(final BlobContainerClient containerClient) {
        this(containerClient, false);
    }

    public void ensureWorking() {
        if (!containerClient.exists()) {
            throw new RuntimeException( // NOPMD
                    "Cannot access Azure Blob container \"" + containerClient.getBlobContainerName() + "\"");
        }
    }

    public List<String> getBlobNames() {
        if (cache) {
            if (blobNamesCache == null) {
                blobNamesCache = getBlobNames0();
            }
            return blobNamesCache;
        } else {
            return getBlobNames0();
        }
    }

    private List<String> getBlobNames0() {
        return StreamSupport.stream(containerClient.listBlobs().spliterator(), false)
                .map(BlobItem::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Get blob names with the prefix.
     *
     * <p>
     * Doesn't support caching.
     */
    public List<String> getBlobNames(final String prefix) {
        Objects.requireNonNull(prefix, "prefix cannot be null");

        return StreamSupport.stream(containerClient.listBlobsByHierarchy(prefix).spliterator(), false)
                .map(BlobItem::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    public void clear(final String prefix) {
        Objects.requireNonNull(prefix, "prefix cannot be null");

        for (final BlobItem blobItem : containerClient.listBlobsByHierarchy(prefix)) {
            final BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
            blobClient.delete();
        }

        if (cache) {
            blobNamesCache = null; // NOPMD
            stringContentCache.clear();
            linesCache.clear();
            decodedLinesCache.clear();
        }
    }

    public String readStringContent(final String blobName, final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        if (cache) {
            return stringContentCache.computeIfAbsent(blobName, k -> readStringContent0(blobName, compression));
        } else {
            return readStringContent0(blobName, compression);
        }
    }

    private String readStringContent0(final String blobName, final String compression) {
        final BlobClient blobClient = containerClient.getBlobClient(blobName);
        final CompressionType compressionType = CompressionType.forName(compression);
        final byte[] blobBytes = blobClient.downloadContent().toBytes();
        try (InputStream decompressedStream = compressionType.decompress(new ByteArrayInputStream(blobBytes));
                InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.readLine();
        } catch (final IOException e) {
            throw new RuntimeException(e); // NOPMD
        }
    }

    public List<String> readLines(final String blobName, final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        if (cache) {
            return linesCache.computeIfAbsent(blobName, k -> readLines0(blobName, compression));
        } else {
            return readLines0(blobName, compression);
        }
    }

    public byte[] readBytes(final String blobName) {
        final BlobClient blobClient = containerClient.getBlobClient(blobName);
        return blobClient.downloadContent().toBytes();
    }

    private List<String> readLines0(final String blobName, final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        final CompressionType compressionType = CompressionType.forName(compression);
        final byte[] blobBytes = readBytes(blobName);
        try (InputStream decompressedStream = compressionType.decompress(new ByteArrayInputStream(blobBytes));
                InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e); // NOPMD
        }
    }

    public List<List<String>> readAndDecodeLines(final String blobName, final String compression,
            final int... fieldsToDecode) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        if (cache) {
            return decodedLinesCache.computeIfAbsent(blobName,
                    k -> readAndDecodeLines0(blobName, compression, fieldsToDecode));
        } else {
            return readAndDecodeLines0(blobName, compression, fieldsToDecode);
        }
    }

    private List<List<String>> readAndDecodeLines0(final String blobName, final String compression,
            final int[] fieldsToDecode) {
        return readLines(blobName, compression).stream()
                .map(l -> l.split(","))
                .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                .collect(Collectors.toList());
    }

    private List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    public List<Record> decodeToRecords(final String blobName, final String compression) {
        return readLines(blobName, compression).stream()
                .map(l -> l.split(","))
                .map(this::decodeRequiredFieldsToRecord)
                .collect(Collectors.toList());
    }

    private Record decodeRequiredFieldsToRecord(final String[] originalFields) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        final String key = b64Decode(originalFields[0]);
        final String value = b64Decode(originalFields[1]);
        final Iterable<Header> headers = decodeHeaders(originalFields[4]);
        return Record.of(key, value, headers);
    }

    public Iterable<Header> decodeHeaders(final String headerValue) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        final String[] headers = headerValue.split(";");
        for (final String header : headers) {
            final String[] keyValue = header.split(":");
            final String key = b64Decode(keyValue[0]);
            final ByteArrayConverter byteArrayConverter = new ByteArrayConverter(); // NOPMD
            final byte[] value = Base64.getDecoder().decode(keyValue[1]);
            final SchemaAndValue schemaAndValue = byteArrayConverter.toConnectHeader("topic0", key, value);
            byteArrayConverter.close();
            connectHeaders.add(key, schemaAndValue);
        }
        return connectHeaders;
    }

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
