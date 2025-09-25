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

import io.aiven.kafka.connect.common.integration.source.AbstractSourceIntegrationTest;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kakfa.connect.s3.source.testdata.AWSIntegrationTestData;
import io.aiven.kakfa.connect.s3.source.testdata.S3SourceStorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Tests the integration with the backend.
 */
@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public final class S3IntegrationTest
        extends
            AbstractSourceIntegrationTest<String, S3Object, S3OffsetManagerEntry, S3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3IntegrationTest.class);

    @Container
    static final LocalStackContainer LOCALSTACK = AWSIntegrationTestData.createS3Container();

    private S3SourceStorage sourceStorage;

    @Override
    protected S3SourceStorage getSourceStorage() {
        return sourceStorage;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAWS() {
        sourceStorage = new S3SourceStorage(new AWSIntegrationTestData(LOCALSTACK));
    }

    @AfterEach
    void tearDownAWS() {
        sourceStorage.cleanup();
    }
}
