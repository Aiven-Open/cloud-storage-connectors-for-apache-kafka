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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.FETCH_PAGE_SIZE;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.START_MARKER_KEY;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public final class S3SourceRecordIterator implements Iterator<S3SourceRecord> {

    public static final Pattern DEFAULT_PATTERN = Pattern.compile("(\\/|^)" // match the / or the start of the key so we
            // shouldn't have to worry about prefix
            + "(?<topic>[^/]+?)-" // assuming no / in topic names
            + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})\\.gz$");
    private String currentKey;
    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = Collections.emptyIterator();

    private final DelimitedRecordReader makeReader;

    private final Map<S3Partition, S3Offset> offsets;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final String s3Prefix;
    private final AmazonS3 s3Client;

    public S3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName, final String s3Prefix,
            final Map<S3Partition, S3Offset> offsets, final DelimitedRecordReader recordReader) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
        this.makeReader = recordReader;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.s3Prefix = s3Prefix;
        try {
            final List<S3ObjectSummary> chunks = fetchObjectSummaries(s3Client);
            nextFileIterator = chunks.iterator();
        } catch (IOException e) {
            throw new AmazonClientException("Failed to initialize S3 file reader", e);
        }
    }

    private List<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) throws IOException {
        final ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName)
//                .withPrefix(s3Prefix)
//                .withMarker(s3SourceConfig.getString(START_MARKER_KEY))
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * 2));

        return new ArrayList<>(objectListing.getObjectSummaries());
    }

    private void nextObject() {
        if (!nextFileIterator.hasNext()) {
            recordIterator = Collections.emptyIterator();
            return;
        }

        try {
            final S3ObjectSummary file = nextFileIterator.next();
            currentKey = file.getKey();
            recordIterator = createIteratorForCurrentFile();
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    private Iterator<ConsumerRecord<byte[], byte[]>> createIteratorForCurrentFile() throws IOException {
        final S3Object s3Object = s3Client.getObject(bucketName, currentKey);
        try (InputStream content = getContent(s3Object);
             BufferedInputStream bufferedContent = new BufferedInputStream(content)) {

            // Extract the topic, partition, and startOffset from the key
//            Matcher matcher = DEFAULT_PATTERN.matcher(currentKey);
//            if (!matcher.find()) {
//                throw new IllegalArgumentException("Invalid file key format: " + currentKey);
//            }
            final String topic = "testtopic";//matcher.group("topic");
            final int partition = 0;//Integer.parseInt(matcher.group("partition"));
            final long startOffset = 0l;//Long.parseLong(matcher.group("offset"));

            return new Iterator<>() {
                private ConsumerRecord<byte[], byte[]> nextRecord = readNext();

                private ConsumerRecord<byte[], byte[]> readNext() {
                    try {
                        return makeReader.read(topic, partition, startOffset, bufferedContent, currentKey);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to read record from file", e);
                    }
                }

                @Override
                public boolean hasNext() {
                    // Check if there's another record
                    return nextRecord != null;
                }

                @Override
                public ConsumerRecord<byte[], byte[]> next() {
                    if (nextRecord == null) {
                        throw new NoSuchElementException();
                    }
                    ConsumerRecord<byte[], byte[]> currentRecord = nextRecord;
                    nextRecord = null;
                    return currentRecord;
                }
            };
        }
    }


    private InputStream getContent(final S3Object object) throws IOException {
        return object.getObjectContent();
    }

    private S3Offset offset() {
        return offsets.get(S3Partition.from(bucketName, s3Prefix, "testtopic", 0));
    }

    @Override
    public boolean hasNext() {
        while (!recordIterator.hasNext() && nextFileIterator.hasNext()) {
            nextObject();
        }
        return recordIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final ConsumerRecord<byte[], byte[]> record = recordIterator.next();
        return new S3SourceRecord(S3Partition.from(bucketName, s3Prefix, record.topic(), record.partition()),
                S3Offset.from(currentKey, record.offset()), record.topic(), record.partition(), record.key(),
                record.value());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
