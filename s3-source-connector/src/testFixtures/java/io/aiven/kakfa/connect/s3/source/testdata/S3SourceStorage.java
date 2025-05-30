package io.aiven.kakfa.connect.s3.source.testdata;

import io.aiven.kafka.connect.common.NativeInfo;
import io.aiven.kafka.connect.common.integration.source.SourceStorage;
import io.aiven.kafka.connect.s3.source.S3SourceConnector;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import org.apache.kafka.connect.connector.Connector;
import org.apache.commons.io.function.IOSupplier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class S3SourceStorage implements SourceStorage<String, S3Object, S3OffsetManagerEntry> {
    AWSIntegrationTestData awsIntegrationTestData;
    BucketAccessor bucketAccessor;

    public S3SourceStorage(final AWSIntegrationTestData awsIntegrationTestData) {
        this.awsIntegrationTestData = awsIntegrationTestData;
        this.bucketAccessor = awsIntegrationTestData.getDefaultBucketAccessor();
    }

    public void tearDown() {
        bucketAccessor.removeBucket();
        awsIntegrationTestData.tearDown();

    }

    @Override
    public String createKey(String prefix, String topic, int partition) {
        return awsIntegrationTestData.createKey(prefix, topic, partition);
    }

    @Override
    public WriteResult<String> writeWithKey(String nativeKey, byte[] testDataBytes) {
        return awsIntegrationTestData.writeWithKey(nativeKey, testDataBytes);
    }

    @Override
    public Map<String, String> createConnectorConfig(String localPrefix) {
        return awsIntegrationTestData.createConnectorConfig(localPrefix, bucketAccessor.getBucketName());
    }

    @Override
    public BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return S3SourceConnector.class;
    }

    @Override
    public void createStorage() {

    }

    @Override
    public void removeStorage() {

    }

    @Override
    public List<? extends NativeInfo<String, S3Object>> getNativeStorage() {
        return List.of();
    }

    @Override
    public IOSupplier<InputStream> getInputStream(String nativeKey) {
        return null;
    }

    @Override
    public String defaultPrefix() {
        return "";
    }
}
