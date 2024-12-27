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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.io.function.IOSupplier;
import org.junit.jupiter.api.AfterEach;
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

    JsonConverter jsonConverter;

    @BeforeEach
    void setUp() {
        jsonConverter = new JsonConverter();
        final Map<String, String> config = new HashMap<>();
        config.put(SCHEMAS_ENABLE, "false");
        jsonConverter.configure(config, false);

        jsonTransformer = new JsonTransformer(jsonConverter);
        sourceCommonConfig = mock(SourceCommonConfig.class);
    }

    @AfterEach
    void destroy() {
        jsonConverter.close();
    }

    @Test
    void testHandleValueDataWithValidJson() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<byte[]> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);

        assertThat(jsonNodes).hasSize(1);
    }

    @Test
    void testHandleValueDataWithValidJsonSkipFew() {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                getJsonRecs(100).getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<byte[]> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 25L);

        final List<byte[]> recs = jsonNodes.collect(Collectors.toList());
        assertThat(recs).hasSize(75);
        assertThat(recs).extracting(record -> ((Map) jsonTransformer.getValueData(record, "", null).value()).get("key"))
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

        final Stream<byte[]> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);

        assertThatThrownBy(() -> jsonTransformer.getValueData(jsonNodes.findAny().get(), "", null))
                .isInstanceOf(DataException.class)
                .hasMessage("Converting byte[] to Kafka Connect data failed due to serialization error: ");
    }

    @Test
    void testSerializeJsonDataValid() throws IOException {
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<byte[]> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, TESTTOPIC, 1,
                sourceCommonConfig, 0);
        final Object serializedData = jsonTransformer
                .getValueData(
                        jsonNodes.findFirst().orElseThrow(() -> new AssertionError("No records found in stream!")),
                        TESTTOPIC, sourceCommonConfig)
                .value();

        // Assert: Verify the serialized data
        assertThat(serializedData).isInstanceOf(Map.class).extracting("key").isEqualTo("value");
    }

    @Test
    void testGetRecordsWithIOException() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException"));
        final Stream<byte[]> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null, 0);

        assertThat(resultStream).isEmpty();
    }

    @Test
    void testCustomSpliteratorWithIOExceptionDuringInitialization() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<byte[]> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, "topic", 0, null, 0);

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
