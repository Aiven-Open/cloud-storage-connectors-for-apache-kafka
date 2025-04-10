///*
// * Copyright 2024 Aiven Oy
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.aiven.kafka.connect.azure.source;
//
//
//import java.util.List;
//import java.util.Map;
//import java.util.function.BiFunction;
//
//import io.aiven.kafka.connect.azure.source.testdata.AzureIntegrationTestData;
//import io.aiven.kafka.connect.azure.source.testdata.AzureOffsetManagerIntegrationTestData;
//import io.aiven.kafka.connect.azure.source.testdata.ContainerAccessor;
//import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
//import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
//import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest;
//
//import org.apache.kafka.connect.connector.Connector;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.testcontainers.containers.GenericContainer;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.junit.jupiter.Testcontainers;
//
//@Testcontainers
//final class AzureBlobClientIntegrationTest extends AbstractIntegrationTest<String, AzureBlobOffsetManagerEntry, AzureBlobSourceRecordIterator> {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobClientIntegrationTest.class);
//
//    @Container
//    private static final GenericContainer<?> AZURITE_CONTAINER = AzureIntegrationTestData.createContainer();
//
//    private AzureIntegrationTestData testData;
//
//    @BeforeEach
//    void setupAzure() {
//        testData = new AzureIntegrationTestData(AZURITE_CONTAINER);
//    }
//
//    @AfterEach
//    void tearDownAzure() {
//        testData.tearDown();
//    }
//
//
////    @BeforeEach
////    void setUp() throws ExecutionException, InterruptedException {
////        testBlobAccessor.clear(azurePrefix);
////        final Map<String, Object> producerProps = new HashMap<>();
////        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
////        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
////                "org.apache.kafka.common.serialization.ByteArraySerializer");
////        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
////                "org.apache.kafka.common.serialization.ByteArraySerializer");
////        super.startConnectRunner(producerProps);
////    }
////
////    @Test
////    void azureBlobClientListsAllBlobs() {
////        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
////        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
////        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
////        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
////        when(config.getAzureContainerName()).thenReturn("test-1");
////        final AzureBlobClient client = new AzureBlobClient(config);
////        // Test the pagintation is automatically executed.
////        when(config.getAzureFetchPageSize()).thenReturn(10);
////        when(config.getAzurePrefix()).thenReturn(azurePrefix);
////
////        final String blobName = "blob-%d.txt";
////        final String contents = "Azure Blob sanity integration and testing %d";
////        for (int i = 0; i < 500; i++) {
////
////            testBlobAccessor.createBlob(azurePrefix, String.format(blobName, i),
////                    new ByteArrayInputStream(String.format(contents, i).getBytes(UTF_8)));// NOPMD
////                                                                                          // avoid
////                                                                                          // instantiating
////                                                                                          // in loops
////        }
////        assertThat(testBlobAccessor.getBlobNames()).hasSize(500);
////
////        assertThat(client.getAzureBlobStream().collect(Collectors.toList())).hasSize(500);
////    }
////
////    @Test
////    void azureBlobClientGetBlobs() {
////        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
////        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
////        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
////        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
////        when(config.getAzureContainerName()).thenReturn("test-1");
////        when(config.getAzurePrefix()).thenReturn(azurePrefix);
////        final AzureBlobClient client = new AzureBlobClient(config);
////        // Test the pagintation is automatically executed.
////        when(config.getAzureFetchPageSize()).thenReturn(1);
////        when(config.getAzurePrefix()).thenReturn(null);
////
////        for (int i = 0; i < 10; i++) {
////            // blob name and contents are the same.
////            testBlobAccessor.createBlob(azurePrefix, "Blob" + i,
////                    new ByteArrayInputStream(("Blob" + i).getBytes(UTF_8)));// NOPMD
////            // avoid
////            // instantiating
////            // in
////            // loops
////        }
////        assertThat(testBlobAccessor.getBlobNames()).hasSize(10);
////
////        client.getAzureBlobStream().forEach(blobitem -> {
////
////            assertThat(blobitem.getName()).isEqualTo(azurePrefix + new String(
////                    Objects.requireNonNull(client.getBlob(blobitem.getName()).blockFirst()).array(), UTF_8));// NOPMD
////            // avoid
////            // instantiating
////            // in
////            // loops
////        });
////
////    }
////
////    @Test
////    void azureBlobClientListBlobsWhenContainerEmpty() {
////        final AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
////        final BlobServiceAsyncClient mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
////        when(config.getAzureServiceAsyncClient()).thenReturn(mockServiceAsyncClient);
////        when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(getAsyncContainerClient());
////        when(config.getAzureContainerName()).thenReturn("test-1");
////        final AzureBlobClient client = new AzureBlobClient(config);
////        when(config.getAzureFetchPageSize()).thenReturn(1);
////        when(config.getAzurePrefix()).thenReturn(null);
////
////        assertThat(client.getAzureBlobStream()).isNullOrEmpty();
////
////    }
//
//    @Override
//    protected Logger getLogger() {
//        return LOGGER;
//    }
//
//    @Override
//    protected String createKey(String prefix, String topic, int partition) {
//        return testData.createKey(prefix, topic, partition);
//    }
//
//    @Override
//    protected WriteResult<String> writeWithKey(String nativeKey, byte[] testDataBytes) {
//        return testData.writeWithKey(nativeKey, testDataBytes);
//    }
//
//    @Override
//    protected List<ContainerAccessor.AzureNativeInfo> getNativeStorage() {
//        return testData.getNativeStorage();
//    }
//
//    @Override
//    protected Class<? extends Connector> getConnectorClass() {
//        return testData.getConnectorClass();
//    }
//
//    @Override
//    protected Map<String, String> createConnectorConfig(String localPrefix) {
//        return testData.createConnectorConfig(localPrefix);
//    }
//
//    @Override
//    protected BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
//        return AzureOffsetManagerIntegrationTestData.offsetManagerEntryFactory();
//    }
//
//}
