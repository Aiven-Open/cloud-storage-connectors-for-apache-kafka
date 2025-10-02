package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.Blob;

import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.integration.sink.AbstractSinkIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.RecordProducer;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;



@Testcontainers
public class GCSIntegrationTest extends AbstractSinkIntegrationTest<String> {

    @Container
    FakeGcsServerContainer gcsServerContainer = new FakeGcsServerContainer(FakeGcsServerContainer.DEFAULT_IMAGE_NAME);

    GCSSinkStorage sinkStorage;

    @Override
    protected SinkStorage<String, Blob> getSinkStorage() {
        if (sinkStorage == null) {
            sinkStorage = new GCSSinkStorage(gcsServerContainer);
        }
        return sinkStorage;
    }

}
