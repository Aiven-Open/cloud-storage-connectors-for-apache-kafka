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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

import io.aiven.kafka.connect.common.OffsetManager;
import org.apache.kafka.connect.data.SchemaAndValue;
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
    private OffsetManager.OffsetManagerEntry<?> offsetManagerEntry;

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
        when(offsetManagerEntry.getTopic()).thenReturn(TESTTOPIC);
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<SchemaAndValue> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, offsetManagerEntry, sourceCommonConfig);

        assertThat(jsonNodes).hasSize(1);
        verify(offsetManagerEntry, times(1)).incrementRecordCount();
    }

    @Test
    void testHandleValueDataWithValidJsonSkipFew() {
        when(offsetManagerEntry.getTopic()).thenReturn(TESTTOPIC);
        when(offsetManagerEntry.skipRecords()).thenReturn(25L);
        final InputStream validJsonInputStream = new ByteArrayInputStream(
                getJsonRecs(100).getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
        final Stream<SchemaAndValue> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, offsetManagerEntry, sourceCommonConfig);

        final List<Object> recs = jsonNodes.map(SchemaAndValue::value).collect(Collectors.toList());
        assertThat(recs).hasSize(75);
        verify(offsetManagerEntry, times(100)).incrementRecordCount();
        assertThat(recs).extracting(record -> ((Map)record).get("key"))
                .doesNotContain("value1")
                .doesNotContain("value2")
                .doesNotContain("value25")
                .contains("value26")
                .contains("value27")
                .contains("value100");
    }

    @Test
    void testHandleValueDataWithInvalidJson() {
        when(offsetManagerEntry.getTopic()).thenReturn(TESTTOPIC);
        final InputStream invalidJsonInputStream = new ByteArrayInputStream(
                "invalid-json".getBytes(StandardCharsets.UTF_8));
        final IOSupplier<InputStream> inputStreamIOSupplier = () -> invalidJsonInputStream;

        final Stream<SchemaAndValue> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, offsetManagerEntry, sourceCommonConfig);

        assertThat(jsonNodes.count()).isEqualTo(0);
        verify(offsetManagerEntry, times(0)).incrementRecordCount();
    }
//
//    @Test
//    void testSerializeJsonDataValid() throws IOException {
//        final InputStream validJsonInputStream = new ByteArrayInputStream(
//                "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8));
//        final IOSupplier<InputStream> inputStreamIOSupplier = () -> validJsonInputStream;
//        final Stream<SchemaAndValue> jsonNodes = jsonTransformer.getRecords(inputStreamIOSupplier, offsetManagerEntry, sourceCommonConfig);
//        final Object serializedData = jsonTransformer
//                .getValueData(
//                        jsonNodes.findFirst().orElseThrow(() -> new AssertionError("No records found in stream!")),
//                        TESTTOPIC, sourceCommonConfig)
//                .value();
//
//        // Assert: Verify the serialized data
//        assertThat(serializedData).isInstanceOf(Map.class).extracting("key").isEqualTo("value");
//    }

    @Test
    void testGetRecordsWithIOException() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException"));
        final Stream<SchemaAndValue> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, offsetManagerEntry, sourceCommonConfig);

        assertThat(resultStream).isEmpty();
        verify(offsetManagerEntry, times(0)).incrementRecordCount();
    }

    @Test
    void testCustomSpliteratorWithIOExceptionDuringInitialization() throws IOException {
        when(inputStreamIOSupplierMock.get()).thenThrow(new IOException("Test IOException during initialization"));
        final Stream<SchemaAndValue> resultStream = jsonTransformer.getRecords(inputStreamIOSupplierMock, offsetManagerEntry, sourceCommonConfig);

        assertThat(resultStream).isEmpty();
        verify(offsetManagerEntry, times(0)).incrementRecordCount();

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
