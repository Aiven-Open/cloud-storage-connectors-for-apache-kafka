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

package io.aiven.kafka.connect.azure.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.azure.source.testdata.AzureSourceIntegrationTestData;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecord;
import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.utils.CasedString;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@DisabledOnOs(value = { OS.WINDOWS, OS.MAC }, disabledReason = "Container testing does not run on Mac and Windows")
@Testcontainers
final class AzureBlobSourceTaskTest {

    /** The default timeout when polling for records */
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    /**
     * The default polling interval.
     */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    /**
     * The Test info provided before each test. Tests may access this info wihout capturing it themselves.
     */
    private TestInfo testInfo;

    /**
     * The amount of extra time that we will allow for timing errors.
     */
    private static final long TIMING_DELTA = 500;

    private static final String TEST_BUCKET = "test-bucket";

    private static final String TEST_OBJECT_KEY = "object_key";

    @Container
    private static final AzuriteContainer AZURITE_CONTAINER = AzureSourceIntegrationTestData.createContainer();

    private AzureSourceIntegrationTestData testData;

    /**
     * Sets up the Azure test container.
     *
     * @param testInfo
     *            the test info.
     */
    @BeforeEach
    void setupAWS(final TestInfo testInfo) {
        this.testInfo = testInfo;
        testData = new AzureSourceIntegrationTestData(AZURITE_CONTAINER);
    }

    @AfterEach
    void tearDownAWS() {
        testData.releaseResources();
    }

    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    public String getTopic() {
        return testInfo.getTestMethod().get().getName();
    }

    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    public String getContainer() {
        return new CasedString(CasedString.StringCase.CAMEL, testInfo.getTestMethod().get().getName())
                .toCase(CasedString.StringCase.KEBAB)
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Creates a mock source context that has no data.
     *
     * @return the mock SourceTaskContext.
     */
    private SourceTaskContext createSourceTaskContext() {
        final SourceTaskContext mockedSourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader mockedOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockedSourceTaskContext.offsetStorageReader()).thenReturn(mockedOffsetStorageReader);
        return mockedSourceTaskContext;
    }

    /**
     * Creates a default configuration data map.
     *
     * @return the default configuration data map.
     */
    private Map<String, String> createDefaultConfig() {
        final Map<String, String> props = testData.createConnectorConfig(null, getContainer());
        final String name = new CasedString(CasedString.StringCase.CAMEL, TestingAzureSourceTask.class.getSimpleName())
                .toCase(CasedString.StringCase.KEBAB)
                .toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();

        TransformerFragment.setter(props).inputFormat(InputFormat.BYTES);
        KafkaFragment.setter(props)
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class)
                .tasksMax(1)
                .name(name);
        CommonConfigFragment.setter(props).taskId(0);
        SourceConfigFragment.setter(props).targetTopic(getTopic()).maxPollRecords(50);
        return props;
    }

    @Test
    void testGetIterator() {
        final List<AzureBlobSourceRecord> records = createAzureSourceRecords(5);
        final TestingAzureSourceTask azureSourceTask = new TestingAzureSourceTask(records.iterator());

        azureSourceTask.initialize(createSourceTaskContext());
        azureSourceTask.start(createDefaultConfig());
        await().atMost(TIMEOUT).until(azureSourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = azureSourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(result).hasSize(5);
        });
        assertThat(result).hasSize(5);
    }

    private static AzureBlobSourceRecord createAzureSourceRecord(final String container, final String objectKey,
            final byte[] key, final byte[] value) {

        final BlobItem blobItem = new BlobItem();
        blobItem.setName(objectKey);
        final BlobItemProperties blobItemProperties = new BlobItemProperties();
        blobItemProperties.setContentLength((long) value.length);
        blobItem.setProperties(blobItemProperties);
        final AzureBlobSourceRecord result = new AzureBlobSourceRecord(blobItem);
        result.setOffsetManagerEntry(new AzureBlobOffsetManagerEntry(container, objectKey));
        result.setKeyData(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, key));
        result.setValueData(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));
        final Context<String> context = new Context<>(objectKey);
        context.setTopic("topic");
        context.setPartition(null);
        result.setContext(context);
        return result;
    }

    private List<AzureBlobSourceRecord> createAzureSourceRecords(final int count) {
        final List<AzureBlobSourceRecord> lst = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            lst.add(createAzureSourceRecord(TEST_BUCKET, TEST_OBJECT_KEY,
                    ("Hello " + i).getBytes(StandardCharsets.UTF_8),
                    ("Hello World" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return lst;
    }

    private static class TestingAzureSourceTask extends AzureBlobSourceTask { // NOPMD not a test class

        private final Iterator<AzureBlobSourceRecord> sourceRecordIterator;

        TestingAzureSourceTask(final Iterator<AzureBlobSourceRecord> realIterator) {
            super(realIterator);
            this.sourceRecordIterator = realIterator;
        }

        public SourceTaskContext getContext() {
            return context;
        }

        @Override
        protected SourceCommonConfig configure(final Map<String, String> props) {
            final SourceCommonConfig cfg = super.configure(props);
            if (sourceRecordIterator != null) {
                super.azureBlobSourceRecordIterator = sourceRecordIterator;
            }
            return cfg;
        }
    }
}
