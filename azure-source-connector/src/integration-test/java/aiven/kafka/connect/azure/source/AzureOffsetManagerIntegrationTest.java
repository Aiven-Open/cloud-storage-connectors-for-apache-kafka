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

import io.aiven.kafka.connect.azure.AzureStorage;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecord;
import io.aiven.kafka.connect.common.integration.source.AbstractOffsetManagerIntegrationTest;

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
public class AzureOffsetManagerIntegrationTest
        extends
            AbstractOffsetManagerIntegrationTest<String, BlobItem, AzureBlobOffsetManagerEntry, AzureBlobSourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureOffsetManagerIntegrationTest.class);

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
}
