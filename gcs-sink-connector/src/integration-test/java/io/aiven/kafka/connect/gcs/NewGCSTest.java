package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.Blob;

import io.aiven.kafka.connect.common.integration.sink.AbstractSinkIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;



@Testcontainers
public class NewGCSTest extends AbstractSinkIntegrationTest<String, Blob> {

    @Container
    FakeGcsServerContainer gcsServerContainer = new FakeGcsServerContainer(FakeGcsServerContainer.DEFAULT_IMAGE_NAME);


//    @SuppressWarnings("rawtypes")
//    private static final GenericContainer<?> FAKE_GCS_CONTAINER = new FixedHostPortGenericContainer(
//            String.format("fsouza/fake-gcs-server:%s", FAKE_GCS_SERVER_VERSION))
//            .withFixedExposedPort(GCS_PORT, GCS_PORT)
//            .withCommand("-port", Integer.toString(GCS_PORT), "-scheme", "http")
//            .withReuse(true);
//
//    static int getRandomPort() {
//        try (ServerSocket socket = new ServerSocket(0)) {
//            return socket.getLocalPort();
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to allocate port for test GCS container", e); // NOPMD throwing raw
//            // exception
//        }
//    }

    GCSSinkStorage sinkStorage;


    @Override
    protected SinkStorage<String, Blob> getSinkStorage() {
        if (sinkStorage == null) {
            sinkStorage = new GCSSinkStorage(gcsServerContainer);
        }
        return sinkStorage;
    }



}
