/*
 * Copyright 2025 Aiven Oy
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

package aiven.kafka.connect.azure.source;

import java.util.Map;

import io.aiven.kafka.connect.azure.AzureStorage;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecord;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
import io.aiven.kafka.connect.common.integration.source.AbstractSourceIteratorIntegrationTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;

import com.azure.storage.blob.models.BlobItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public class AzureSourceRecordIteratorIntegrationTest
        extends
            AbstractSourceIteratorIntegrationTest<String, BlobItem, AzureBlobOffsetManagerEntry, AzureBlobSourceRecord, AzureBlobSourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureSourceRecordIteratorIntegrationTest.class);

    @Container
    static final AzuriteContainer LOCALSTACK = AzureStorage.createContainer();

    private AzureSourceStorage sourceStorage;

    @Override
    protected AzureSourceStorage getSourceStorage() {
        return sourceStorage;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAWS() {
        sourceStorage = new AzureSourceStorage(LOCALSTACK);
    }

    @AfterEach
    void tearDownAWS() {
        sourceStorage.cleanup();
    }

    @Override
    protected AzureBlobSourceRecordIterator getSourceRecordIterator(final Map<String, String> configData,
            final OffsetManager<AzureBlobOffsetManagerEntry> offsetManager, final Transformer transformer) {
        final AzureBlobSourceConfig sourceConfig = new AzureBlobSourceConfig(configData);
        return new AzureBlobSourceRecordIterator(sourceConfig, offsetManager, transformer,
                new AzureBlobClient(sourceConfig));
    }
    // @Container
    // static final LocalStackContainer LOCALSTACK = AWSIntegrationTestData.createS3Container();
    //
    // private AWSIntegrationTestData testData;
    // private BucketAccessor bucketAccessor;
    //
    // @Override
    // protected Logger getLogger() {
    // return LOGGER;
    // }
    //
    // @BeforeEach
    // void setupAWS() {
    // testData = new AWSIntegrationTestData(LOCALSTACK);
    // bucketAccessor = testData.getDefaultBucketAccessor();
    // }
    //
    // @AfterEach
    // void tearDownAWS() {
    // bucketAccessor.removeBucket();
    // testData.tearDown();
    // }
    //
    // @Override
    // protected String createKey(final String prefix, final String topic, final int partition) {
    // return testData.createKey(prefix, topic, partition);
    // }
    //
    // @Override
    // protected List<BucketAccessor.S3NativeInfo> getNativeStorage() {
    // return bucketAccessor.getNativeStorage();
    // }
    //
    // @Override
    // protected Class<? extends Connector> getConnectorClass() {
    // return testData.getConnectorClass();
    // }
    //
    // @Override
    // protected WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
    // return testData.writeWithKey(nativeKey, testDataBytes);
    // }
    //
    // @Override
    // protected Map<String, String> createConnectorConfig(final String localPrefix) {
    // return testData.createConnectorConfig(localPrefix, bucketAccessor.getBucketName());
    // }
    //
    // @Override
    // protected BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory()
    // {
    // return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    // }
    //
    // @Override
    // protected S3SourceRecordIterator getSourceRecordIterator(final Map<String, String> configData,
    // final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer) {
    // final S3SourceConfig sourceConfig = new S3SourceConfig(configData);
    // return new S3SourceRecordIterator(sourceConfig, offsetManager, transformer,
    // new AWSV2SourceClient(sourceConfig));
    // }
}
