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

package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.Blob;
import io.aiven.kafka.connect.common.integration.sink.AbstractSinkAvroIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.AbstractSinkParquetIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
@Testcontainers
final class GCSParquetIntegrationTest extends AbstractSinkParquetIntegrationTest<String, Blob> {

    @Container
    FakeGcsServerContainer gcsServerContainer = new FakeGcsServerContainer(FakeGcsServerContainer.DEFAULT_IMAGE_NAME);

    private GCSSinkStorage sinkStorage;

    @Override
    protected SinkStorage<String, Blob> getSinkStorage() {
        if (sinkStorage == null) {
            sinkStorage = new GCSSinkStorage(gcsServerContainer);
        }
        return sinkStorage;
    }
}
