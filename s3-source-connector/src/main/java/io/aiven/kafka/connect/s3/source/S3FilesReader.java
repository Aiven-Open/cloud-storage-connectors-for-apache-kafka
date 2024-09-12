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

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Helpers for reading records out of S3. Not thread safe. Records should be in order since S3 lists files in
 * lexicographic order. It is strongly recommended that you use a unique key prefix per topic as there is no option to
 * restrict this reader by topic.
 * <p>
 * NOTE: hasNext() on the returned iterators may throw AmazonClientException if there was a problem communicating with
 * S3 or reading an object. Your code should catch AmazonClientException and implement back-off and retry as desired.
 * <p>
 * Any other exception should be considered a permanent failure.
 */
public class S3FilesReader implements Iterable<S3SourceRecord> {

    public static final Pattern DEFAULT_PATTERN = Pattern.compile("(\\/|^)" // match the / or the start of the key so we
                                                                            // shouldn't have to worry about prefix
            + "(?<topic>[^/]+?)-" // assuming no / in topic names
            + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})\\.gz$");

    private final AmazonS3 s3Client;

    private final DelimitedRecordReader makeReader;

    private final Map<S3Partition, S3Offset> offsets;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final String s3Prefix;

    public S3FilesReader(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final String s3Prefix, final Map<S3Partition, S3Offset> offsets, final DelimitedRecordReader recordReader) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
        this.s3Client = s3Client;
        this.makeReader = recordReader;
        this.bucketName = bucketName;
        this.s3Prefix = s3Prefix;
    }

    @Override
    public Iterator<S3SourceRecord> iterator() {
        return readAll();
    }

    public Iterator<S3SourceRecord> readAll() {
        return new Iterator<>() {
            String currentKey;
            Iterator<S3ObjectSummary> nextFile;
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = Collections.emptyIterator();

            // Initialize once by listing all objects matching the criteria
            {
                // Fetch all objects in one go
                final ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest(bucketName, s3Prefix,
                        s3SourceConfig.getString(START_MARKER_KEY), null, s3SourceConfig.getInt(FETCH_PAGE_SIZE) * 2));

                // Filter and collect the relevant object summaries
                final List<S3ObjectSummary> chunks = new ArrayList<>(objectListing.getObjectSummaries());

                // Set up the iterator for files
                nextFile = chunks.iterator();
            }

            private void nextObject() {
                if (!nextFile.hasNext()) {
                    iterator = Collections.emptyIterator();
                    return;
                }

                try {
                    final S3ObjectSummary file = nextFile.next();
                    currentKey = file.getKey();
                    try (InputStream content = getContent(s3Client.getObject(bucketName, currentKey))) {
                        iterator = parseKey(currentKey, (topic, partition, startOffset) -> makeReader.readAll(topic,
                                partition, content, startOffset));
                    }
                } catch (IOException e) {
                    throw new AmazonClientException(e);
                }
            }

            private InputStream getContent(final S3Object object) throws IOException {
                return object.getObjectContent();
            }

            @Override
            public boolean hasNext() {
                while (!iterator.hasNext() && nextFile.hasNext()) {
                    nextObject();
                }
                return iterator.hasNext();
            }

            @Override
            public S3SourceRecord next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final ConsumerRecord<byte[], byte[]> record = iterator.next();
                return new S3SourceRecord(S3Partition.from(bucketName, s3Prefix, record.topic(), record.partition()),
                        S3Offset.from(currentKey, record.offset()), record.topic(), record.partition(), record.key(),
                        record.value());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private <T> T parseKey(final String key, final KeyConsumer<T> consumer) throws IOException {
        final Matcher matcher = DEFAULT_PATTERN.matcher(key);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Not a valid chunk filename! " + key);
        }
        final String topic = matcher.group("topic");
        final int partition = Integer.parseInt(matcher.group("partition"));
        final long startOffset = Long.parseLong(matcher.group("offset"));

        return consumer.consume(topic, partition, startOffset);
    }

    private interface KeyConsumer<T> {
        T consume(String topic, int partition, long startOffset) throws IOException;
    }

}
