package io.aiven.kafka.connect.azure.source;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Binds;
import io.aiven.kafka.connect.azure.source.testdata.AzureIntegrationTestData;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.List;

public class AzureIntegrationTestDataTest {

    @Test
    public void test() {
        GenericContainer<?> container = AzureIntegrationTestData.createContainer();
        container.start();
        AzureIntegrationTestData azureIntegrationTestData = new AzureIntegrationTestData(container);
    }
}
