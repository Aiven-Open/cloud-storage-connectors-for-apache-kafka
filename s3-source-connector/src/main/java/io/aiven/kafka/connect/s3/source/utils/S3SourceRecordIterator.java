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

import java.io.InputStream;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class S3SourceRecordIterator
        extends
            AbstractSourceRecordIterator<S3Object, String, S3OffsetManagerEntry, S3SourceRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceRecordIterator.class);

    /** The AWS client that provides the S3Objects */
    private final AWSV2SourceClient sourceClient;

    /** The S3 bucket we are processing */
    private final String bucket;

    /**
     * /** The inner iterator to provides a base S3SourceRecord for an S3Object that has passed the filters and
     * potentially had data extracted.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "source client has stores mutable fields")
    public S3SourceRecordIterator(final S3SourceConfig s3SourceConfig,
            final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer,
            final AWSV2SourceClient sourceClient) {

        super(s3SourceConfig, offsetManager, transformer, s3SourceConfig.getS3FetchBufferSize());
        this.bucket = s3SourceConfig.getAwsS3BucketName();
        this.sourceClient = sourceClient;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    protected Stream<S3Object> getNativeItemStream(final String offset) {
        return sourceClient.getS3ObjectStream(offset);
    }

    @Override
    protected IOSupplier<InputStream> getInputStream(final S3SourceRecord sourceRecord) {
        return sourceClient.getObject(sourceRecord.getNativeKey());
    }

    @Override
    protected String getNativeKey(final S3Object nativeObject) {
        return nativeObject.key();
    }

    @Override
    protected S3SourceRecord createSourceRecord(final S3Object nativeObject) {
        return new S3SourceRecord(nativeObject);
    }

    @Override
    protected S3OffsetManagerEntry createOffsetManagerEntry(final S3Object nativeObject) {
        return new S3OffsetManagerEntry(bucket, nativeObject.key());
    }

    @Override
    protected OffsetManager.OffsetManagerKey getOffsetManagerKey(final String nativeKey) {
        return S3OffsetManagerEntry.asKey(bucket, StringUtils.defaultIfBlank(nativeKey, ""));
    }
}
