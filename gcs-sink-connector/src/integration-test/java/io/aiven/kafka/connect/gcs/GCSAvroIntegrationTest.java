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
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.sink.AbstractSinkAvroIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.AbstractSinkIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
@Testcontainers
final class GCSAvroIntegrationTest extends AbstractSinkAvroIntegrationTest<String, Blob> {

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
