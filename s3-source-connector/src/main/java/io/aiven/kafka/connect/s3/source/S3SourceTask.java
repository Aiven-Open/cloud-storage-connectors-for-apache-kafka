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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.common.source.AbstractSourceTask;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;
import io.aiven.kafka.connect.s3.source.utils.Version;

import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
public final class S3SourceTask extends AbstractSourceTask {
    // TODO Refactor for ease of testing

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    public static final String OBJECT_KEY = "object_key";

    private S3SourceConfig s3SourceConfig;

    private Iterator<S3SourceRecord> sourceRecordIterator;
    private Optional<Converter> keyConverter;

    private Converter valueConverter;

    private Transformer transformer;

    private AWSV2SourceClient awsv2SourceClient;
    private final Set<String> failedObjectKeys = new HashSet<>();

    private OffsetManager offsetManager;

    public S3SourceTask() {
        super(LOGGER);
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void configure(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        s3SourceConfig = new S3SourceConfig(props);
        initializeConverters();
        this.transformer = TransformerFactory.getTransformer(s3SourceConfig);
        offsetManager = new OffsetManager(context, s3SourceConfig);
        awsv2SourceClient = new AWSV2SourceClient(s3SourceConfig, failedObjectKeys);
        sourceRecordIterator = prepareReaderFromOffsetStorageReader();
    }

    @Override
    public Iterator<SourceRecord> getIterator() {
        return IteratorUtils.transformedIterator(sourceRecordIterator,
                s3SourceRecord -> createSourceRecord(s3SourceRecord));
    }

    private void initializeConverters() {
        try {
            keyConverter = Optional
                    .of((Converter) Class.forName((String) s3SourceConfig.originals().get("key.converter"))
                            .getDeclaredConstructor()
                            .newInstance());
            valueConverter = (Converter) Class.forName((String) s3SourceConfig.originals().get("value.converter"))
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new ConnectException("Connect converters could not be instantiated.", e);
        }
    }

    Iterator<S3SourceRecord> prepareReaderFromOffsetStorageReader() {
        return sourceRecordIterator = new SourceRecordIterator(s3SourceConfig, offsetManager, this.transformer,
                awsv2SourceClient);
    }

    @Override
    protected void closeResources() {
        awsv2SourceClient.shutdown();
    }

    // below for visibility in tests
    Optional<Converter> getKeyConverter() {
        return keyConverter;
    }

    Converter getValueConverter() {
        return valueConverter;
    }

    Transformer getTransformer() {
        return transformer;
    }

    /**
     * Creates a Source record from the S3SourceRecord. Package private to support testing. Updates the
     * {@link OffsetManager} to indicate the record has been processed.
     *
     * @param s3SourceRecord
     *            the S3SourceRecord to convert.
     * @return a SourceRecord based on the S3SourceRecord.
     */
    SourceRecord createSourceRecord(final S3SourceRecord s3SourceRecord) {
        final Map<String, String> conversionConfig = new HashMap<>();

        final String topic = s3SourceRecord.getTopic();
        final Optional<SchemaAndValue> keyData = keyConverter.map(c -> c.toConnectData(topic, s3SourceRecord.key()));

        transformer.configureValueConverter(conversionConfig, s3SourceConfig);
        valueConverter.configure(conversionConfig, false);
        try {
            final SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, s3SourceRecord.value());
            offsetManager.updateCurrentOffsets(s3SourceRecord.getPartitionMap(), s3SourceRecord.getOffsetMap());
            s3SourceRecord.setOffsetMap(offsetManager.getOffsets().get(s3SourceRecord.getPartitionMap()));
            return s3SourceRecord.getSourceRecord(topic, keyData, schemaAndValue);
        } catch (DataException e) {
            LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
            awsv2SourceClient.addFailedObjectKeys(s3SourceRecord.getObjectKey());
            throw e;
        }
    }

}
