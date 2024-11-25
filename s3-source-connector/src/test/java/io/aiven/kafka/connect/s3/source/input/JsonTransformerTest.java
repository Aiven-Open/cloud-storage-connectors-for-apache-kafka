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

package io.aiven.kafka.connect.s3.source.input;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMAS_ENABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class JsonTransformerTest {

    public static final String TESTTOPIC = "testtopic";
    JsonTransformer jsonTransformer;

    S3SourceConfig s3SourceConfig;

    @Mock
    private IOSupplier<InputStream> inputStreamIOSupplierMock;

    @BeforeEach
    void setUp() {
        jsonTransformer = new JsonTransformer();
        s3SourceConfig = mock(S3SourceConfig.class);
    }

    @Test
    void testConfigureValueConverter() {
        final Map<String, String> config = new HashMap<>();

        jsonTransformer.configureValueConverter(config, s3SourceConfig);
        assertThat(config).as("%s should be set to false", SCHEMAS_ENABLE)
                .containsEntry(SCHEMAS_ENABLE, Boolean.FALSE.toString());
    }

    @Test
    void testHandleValueDataWithValidJson() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<Object> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                s3SourceConfig);

        assertThat(jsonNodes).hasSize(1);
    }

    @Test
    void testHandleValueDataWithInvalidJson() {
        final InputStream invalidJsonInputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> invalidJsonInputStream;

        final Stream<Object> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                s3SourceConfig);

        assertThat(jsonNodes).isEmpty();
    }

    @Test
    void testSerializeJsonDataValid() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<Object> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                s3SourceConfig);
        final byte[] serializedData = jsonTransformer.getValueBytes(jsonNodes.findFirst().get(), TESTTOPIC,
                s3SourceConfig);

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode expectedData = objectMapper.readTree(serializedData);

        assertThat(expectedData.get("key").asText()).isEqualTo("value");
    }

    @Test
    void testGetRecordsWithIOException() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException"));
        final Stream<Object> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null);

        assertThat(resultStream).isEmpty();
    }

    @Test
    void testCustomSpliteratorStreamProcessing() throws IOException {
        final String jsonContent = "{\"key\":\"value\"}\n{\"key2\":\"value2\"}";
        final InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> supplier = () -> inputStream;

        final JsonTransformer.CustomSpliterator spliterator = jsonTransformer.new CustomSpliterator(supplier);
        assertThat(spliterator.tryAdvance(jsonNode -> assertThat(jsonNode).isNotNull())).isTrue();
    }

    @Test
    void testCustomSpliteratorWithIOExceptionDuringInitialization() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<Object> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null);

        assertThat(resultStream).isEmpty();
    }
}
