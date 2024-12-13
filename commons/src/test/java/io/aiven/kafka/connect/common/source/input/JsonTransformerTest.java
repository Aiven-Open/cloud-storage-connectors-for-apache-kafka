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

package io.aiven.kafka.connect.common.source.input;

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMAS_ENABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

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

    SourceCommonConfig sourceCommonConfig;

    @Mock
    private IOSupplier<InputStream> inputStreamIOSupplierMock;

    @BeforeEach
    void setUp() {
        jsonTransformer = new JsonTransformer();
        sourceCommonConfig = mock(SourceCommonConfig.class);
    }

    @Test
    void testConfigureValueConverter() {
        final Map<String, String> config = new HashMap<>();

        jsonTransformer.configureValueConverter(config, sourceCommonConfig);
        assertThat(config).as("%s should be set to false", SCHEMAS_ENABLE)
                .containsEntry(SCHEMAS_ENABLE, Boolean.FALSE.toString());
    }

    @Test
    void testHandleValueDataWithValidJson() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<JsonNode> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);

        assertThat(jsonNodes).hasSize(1);
    }

    @Test
    void testHandleValueDataWithValidJsonSkipFew() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                getJsonRecs(100).getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<JsonNode> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 25L);

        final List<Object> recs = jsonNodes.collect(Collectors.toList());
        assertThat(recs).hasSize(75);
        assertThat(recs).extracting(record -> ((JsonNode) record).get("key").asText())
                .doesNotContain("value1")
                .doesNotContain("value2")
                .doesNotContain("value25")
                .contains("value26")
                .contains("value27")
                .contains("value100");
    }

    @Test
    void testHandleValueDataWithInvalidJson() {
        final InputStream invalidJsonInputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> invalidJsonInputStream;

        final Stream<JsonNode> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);

        assertThat(jsonNodes).isEmpty();
    }

    @Test
    void testSerializeJsonDataValid() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<JsonNode> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);
        final byte[] serializedData = jsonTransformer.getValueBytes(jsonNodes.findFirst().get(), TESTTOPIC,
                sourceCommonConfig);

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode expectedData = objectMapper.readTree(serializedData);

        assertThat(expectedData.get("key").asText()).isEqualTo("value");
    }

    @Test
    void testGetRecordsWithIOException() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException"));
        final Stream<JsonNode> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null, 0);

        assertThat(resultStream).isEmpty();
    }

    // @Test
    // void testCustomSpliteratorStreamProcessing() throws IOException {
    // final String jsonContent = "{\"key\":\"value\"}\n{\"key2\":\"value2\"}";
    // final InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8));
    // final IOSupplier<InputStream> supplier = () -> inputStream;
    //
    // final JsonTransformer.CustomSpliterator spliterator = jsonTransformer.new CustomSpliterator(supplier);
    // assertThat(spliterator.tryAdvance(jsonNode -> assertThat(jsonNode).isNotNull())).isTrue();
    // }

    @Test
    void testCustomSpliteratorWithIOExceptionDuringInitialization() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<JsonNode> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null, 0);

        assertThat(resultStream).isEmpty();
    }

    static String getJsonRecs(final int recordCount) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 1; i <= recordCount; i++) {
            jsonRecords.append(String.format("{\"key\":\"value%d\"}", i));
            if (i < recordCount) {
                jsonRecords.append("\n"); // NOPMD AppendCharacterWithChar
            }
        }
        return jsonRecords.toString();
    }
}
