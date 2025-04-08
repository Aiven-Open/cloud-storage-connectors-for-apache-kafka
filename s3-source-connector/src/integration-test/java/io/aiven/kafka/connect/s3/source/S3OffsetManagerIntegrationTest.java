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

package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.integration.AbstractOffsetManagerIntegrationTest;
import io.aiven.kakfa.connect.s3.source.testdata.AWSIntegrationTestData;
import io.aiven.kakfa.connect.s3.source.testdata.S3OffsetManagerIntegrationTestData;
import io.aiven.kakfa.connect.s3.source.testdata.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecordIterator;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public class S3OffsetManagerIntegrationTest
        extends
            AbstractOffsetManagerIntegrationTest<String, S3OffsetManagerEntry, S3SourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3OffsetManagerIntegrationTest.class);

    @Container
    static final LocalStackContainer LOCALSTACK = AWSIntegrationTestData.createS3Container();

    private AWSIntegrationTestData testData;

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAWS() {
        testData = new AWSIntegrationTestData(LOCALSTACK);
    }

    @AfterEach
    void tearDownAWS() {
        testData.tearDown();
    }

    @Override
    protected String createKey(final String prefix, final String topic, final int partition) {
        return testData.createKey(prefix, topic, partition);
    }

    @Override
    protected List<BucketAccessor.S3NativeInfo> getNativeStorage() {
        return testData.getNativeStorage();
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return testData.getConnectorClass();
    }

    @Override
    protected WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        return testData.writeWithKey(nativeKey, testDataBytes);
    }

    @Override
    protected Map<String, String> createConnectorConfig(final String localPrefix) {
        return testData.createConnectorConfig(localPrefix);
    }

    @Override
    protected BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }
}
