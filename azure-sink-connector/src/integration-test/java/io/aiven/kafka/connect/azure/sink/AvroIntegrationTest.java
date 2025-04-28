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

package io.aiven.kafka.connect.azure.sink;

import io.aiven.kafka.connect.common.integration.sink.AbstractAvroIntegrationTest;

import com.azure.storage.blob.models.BlobItem;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public class AvroIntegrationTest extends AbstractAvroIntegrationTest<BlobItem, String> {

    /**
     * The azure container
     */
    @Container
    private static final AzuriteContainer AZURITE_CONTAINER = AzureSinkStorage.createContainer();

    private final AzureSinkStorage storage;

    public AvroIntegrationTest() {
        super();
        storage = new AzureSinkStorage(AZURITE_CONTAINER);
    }
    @Override
    protected AzureSinkStorage getSinkStorage() {
        return storage;
    }
}
