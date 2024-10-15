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

package io.aiven.kafka.connect.s3.source.output;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMAS_ENABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

final class JsonWriterTest {

    JsonWriter jsonWriter;

    @Mock
    OffsetManager offsetManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        jsonWriter = new JsonWriter();
    }

    @Test
    void testConfigureValueConverter() {
        final Map<String, String> config = new HashMap<>();
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);

        jsonWriter.configureValueConverter(config, s3SourceConfig);
        assertEquals("false", config.get(SCHEMAS_ENABLE), "SCHEMAS_ENABLE should be set to false");
    }

    @Test
    void testHandleValueDataWithValidJson() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final List<Object> jsonNodes = jsonWriter.getRecords(validJsonInputStream, "testtopic", 1, s3SourceConfig);

        assertThat(jsonNodes.size()).isEqualTo(1);
    }

    @Test
    void testHandleValueDataWithInvalidJson() {
        final InputStream invalidJsonInputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);

        final List<Object> jsonNodes = jsonWriter.getRecords(invalidJsonInputStream, "testtopic", 1, s3SourceConfig);

        assertThat(jsonNodes.size()).isEqualTo(0);
    }

    @Test
    void testSerializeJsonDataValid() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final List<Object> jsonNodes = jsonWriter.getRecords(validJsonInputStream, "testtopic", 1, s3SourceConfig);

        final byte[] serializedData = jsonWriter.getValueBytes(jsonNodes.get(0), "testtopic", s3SourceConfig);

        final ObjectMapper objectMapper = new ObjectMapper();

        final JsonNode expectedData = objectMapper.readTree(serializedData);

        assertThat(expectedData.get("key").asText()).isEqualTo("value");
    }
}
