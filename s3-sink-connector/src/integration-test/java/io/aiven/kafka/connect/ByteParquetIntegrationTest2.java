package io.aiven.kafka.connect;

import com.amazonaws.services.s3.model.S3Object;
import io.aiven.kafka.connect.common.integration.sink.AbstractByteParquetIntegrationTest;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ByteParquetIntegrationTest2 extends AbstractByteParquetIntegrationTest<S3Object, String> {

    @Container
    public static final LocalStackContainer LOCALSTACK = S3SinkStorage.createContainer();

    @Override
    protected SinkStorage<S3Object, String> getSinkStorage() {
        return new S3SinkStorage(LOCALSTACK);
    }
}