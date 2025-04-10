//package io.aiven.kafka.connect.azure.source.testing;
//
//import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
//import io.aiven.kafka.connect.azure.source.testdata.AzureIntegrationTestData;
//import io.aiven.kafka.connect.azure.source.testdata.AzureOffsetManagerIntegrationTestData;
//import io.aiven.kafka.connect.azure.source.testdata.ContainerAccessor;
//import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;
//import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
//import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
//import io.aiven.kafka.connect.common.integration.AbstractSourceIteratorIntegrationTest;
//import io.aiven.kafka.connect.common.source.OffsetManager;
//import io.aiven.kafka.connect.common.source.input.Transformer;
//import org.apache.kafka.connect.connector.Connector;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.testcontainers.containers.GenericContainer;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.junit.jupiter.Testcontainers;
//
//import java.util.List;
//import java.util.Map;
//import java.util.function.BiFunction;
//
//@Testcontainers
//public class AzureiteContainerTest {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(AzureiteContainerTest.class);
//
//    @Container
//    private static final GenericContainer<?> AZURITE_CONTAINER = AzureIntegrationTestData.createContainer();
//
//    private AzureIntegrationTestData testData;
//
//
////    @BeforeEach
////    void setupAzure() {
////        testData = new AzureIntegrationTestData(AZURITE_CONTAINER);
////    }
////
////    @AfterEach
////    void tearDownAzure() {
////        testData.tearDown();
////    }
////
//    @Test
//    void x() {
//        testData = new AzureIntegrationTestData(AZURITE_CONTAINER);
//        ContainerAccessor containerAccessor = testData.getContainerAccessor("dummy");
//        containerAccessor.createContainer();
//
//    }
//
//
//}
