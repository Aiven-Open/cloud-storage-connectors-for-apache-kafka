/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.integration.sink;


import io.aiven.kafka.connect.common.config.CompressionType;
import org.apache.commons.io.IOUtils;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class BucketAccessor<K extends Comparable<K>> {
    private final String bucketName;

    /**
     * Creates an accessor.
     * @param storage
     * @param bucketName
     */
    protected BucketAccessor(final SinkStorage<K, ?> storage, final String bucketName) {
        Objects.requireNonNull(storage, "storage cannot be null");
        Objects.requireNonNull(bucketName, "bucketName cannot be null");
        this.bucketName = bucketName;
    }

    /**
     * Gets the input stream for an object in this bucket.
     * @param objectKey the key for the object
     * @return the InputStream for the data in the object.
     * @throws IOException if there is an error.
     */
    protected abstract InputStream getInputStream(final K objectKey) throws IOException;

    /**
     * Gets the list of keys for objects in this bucket.
     * @return the list of keys for objects in this bucket.
     * @throws IOException if there is an error.
     */
    protected abstract List<K> listKeys() throws IOException;

    /**
     * Gets the list of keys for objects in this bucket.
     * @return the list of keys for objects in this bucket.
     * @throws IOException if there is an error.
     */
    protected abstract List<K> listKeys(String prefix) throws IOException;

    /**
     * Removes this bucket.
     */
    public abstract void removeBucket();

    /**
     * Creates the bucket;
     */
    public abstract void createBucket();

    /**
     * See if the specific object exists within the bucket.
     * @param objectKey the key for the object.
     * @return {@code true} if the object exits, {@code false} otherwise.
     * @throws IOException on error
     */
    public boolean doesObjectExist(final K objectKey) throws IOException {
        return listKeys().contains(objectKey);
    }


    /**
     * Reads the data from an object and decodes the specified fields before returning the result.
     * @param objectKey the object key to read.
     * @param compression the compression that was used on the object.
     * @param fieldsToDecode the fields to decode
     * @return a List of lists of strings.
     * @throws IOException on error.
     */
    public final List<List<String>> readAndDecodeLines(final K objectKey, final CompressionType compression,
                                                       final int... fieldsToDecode) throws IOException {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        return readLines(objectKey, compression).stream()
                .map(l -> l.split(","))
                .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
                .collect(Collectors.toList());
    }

    /**
     * Reads the bytes from an object key and decompresses if necessary.
     * @param objectKey the key for the object to read.
     * @param compression the compression that was used on the object
     * @return the bytes from the object.
     * @throws IOException on error.
     */
    public final byte[] readBytes(final K objectKey, final CompressionType compression) throws IOException {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        try (InputStream decompressedStream = compression.decompress(getInputStream(objectKey));
             ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream()) {
            IOUtils.copy(decompressedStream, decompressedBytes);
            return decompressedBytes.toByteArray();
        }
    }

    /**
     * Read the bytes from the object without any additional handling.
     * @param objectKey the key for the object to read.
     * @return the bytes from the object.
     * @throws IOException on error.
     */
    public final byte[] readBytes(final K objectKey) throws IOException {
        return readBytes(objectKey, CompressionType.NONE);
    }

    /**
     * Reads the first string from an object.
     * @param objectKey the key for the object to read.
     * @param compression the compression that was used on the object
     * @return the string read from the object.
     * @throws IOException on error.
     */
    public final String readString(final K objectKey, final CompressionType compression) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(readBytes(objectKey, compression));
             InputStreamReader reader = new InputStreamReader(bais, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.readLine();
        }
    }

    /**
     * Read the contents of the object as a collection of lines.
     * @param objectKey the key for the object to read.
     * @param compression the compression that was used on the object
     * @return lines read from the object stream.
     * @throws IOException on error.
     */
    public final List<String> readLines(final K objectKey, final CompressionType compression) throws IOException {
        final byte[] blobBytes = readBytes(objectKey, compression);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
             InputStreamReader reader = new InputStreamReader(bais, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.lines().collect(Collectors.toList());
        }
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

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
