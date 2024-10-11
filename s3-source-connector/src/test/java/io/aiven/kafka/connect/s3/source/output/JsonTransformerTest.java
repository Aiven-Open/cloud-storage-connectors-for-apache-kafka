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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

final class JsonTransformerTest {

    JsonTransformer underTest;

    @Mock
    OffsetManager offsetManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        underTest = new JsonTransformer();
    }

    @Test
    public void testName() {
        assertThat(underTest.getName().equals("json"));
    }

    @Test
    void testConfigureValueConverter() {
        final Map<String, String> config = new HashMap<>();
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);

        underTest.configureValueConverter(config, s3SourceConfig);
        assertEquals("false", config.get(SCHEMAS_ENABLE), "SCHEMAS_ENABLE should be set to false");
    }


    @Test
    void testByteArrayIteratorInvalidData() {
        final InputStream inputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));

        // s3Config is not used by json tranform.
        assertThatThrownBy(() -> underTest.byteArrayIterator(inputStream, "topic", null))
                .isInstanceOf(BadDataException.class);

    }

    @Test
    void testByteArrayIteratorJsonDataValid() throws BadDataException, IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final Iterator<byte[]> iter = underTest.byteArrayIterator(validJsonInputStream, "testtopic", s3SourceConfig);
        assertThat(iter.hasNext()).isTrue();
        byte[] data = iter.next();
        final JsonNode actual = objectMapper.readTree(new ByteArrayInputStream(data));
        assertThat(actual.get("key").asText()).isEqualTo("value");
        assertThat(iter.hasNext()).isFalse();
    }
}
