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

package io.aiven.kafka.connect;

import io.aiven.kafka.connect.common.integration.sink.AbstractAvroIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;

import com.amazonaws.services.s3.model.S3Object;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
@Testcontainers
public class AvroIntegratonTest extends AbstractAvroIntegrationTest<S3Object, String> {

    @Container
    public static final LocalStackContainer LOCALSTACK = S3SinkStorage.createContainer();

    @Override
    protected SinkStorage<S3Object, String> getSinkStorage() {
        return new S3SinkStorage(LOCALSTACK);
    }
}
