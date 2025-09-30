package io.aiven.kafka.connect.gcs;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import io.aiven.kafka.connect.common.NativeInfo;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.sink.BucketAccessor;
import io.aiven.kafka.connect.common.integration.sink.SinkStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.connect.connector.Connector;
import org.codehaus.plexus.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GCSSinkStorage implements SinkStorage<String, Blob> {
    private final Storage storage;
    private final FakeGcsServerContainer gcsServerContainer;

    GCSSinkStorage(FakeGcsServerContainer gcsServerContainer) {
        this.gcsServerContainer = gcsServerContainer;
        this.storage = StorageOptions.newBuilder().setHost(gcsServerContainer.url())
                .setProjectId("gcs-sink-connector")
                .setCredentials(NoCredentials.getInstance())
                .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(1).build())
                .build().getService();
    }

    @Override
    public String getAvroBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        throw new NotImplementedException();
    }

    @Override
    public String getBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        return String.format("%s%s-%d-%d%s", Objects.toString(prefix, ""), topicName, partition, startOffset, compression.extension());
    }

    @Override
    public String getKeyBlobName(String prefix, String key, CompressionType compression) {
        return String.format("%s%s%s", Objects.toString(prefix, ""), key, compression.extension());
    }

    @Override
    public String getTimestampBlobName(String prefix, String topicName, int partition, int startOffset, CompressionType compression) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format("%s%s-%d-%d-%s%s", Objects.toString(prefix, ""), topicName, partition, startOffset,
                time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), compression.extension());
    }

    @Override
    public Map<String, String> createSinkProperties(String bucketName) {
        final Map<String, String> config = new HashMap<>();
//        if (gcsCredentialsPath != null) {
//            config.put("gcs.credentials.path", gcsCredentialsPath);
//        }
//        if (gcsCredentialsJson != null) {
//            config.put("gcs.credentials.json", gcsCredentialsJson);
//        }
//        if (useFakeGCS()) {
//            config.put("gcs.endpoint", gcsEndpoint);
//        }
        config.put("gcs.endpoint", gcsServerContainer.url());
        config.put("gcs.bucket.name", bucketName);
        return config;
    }

    @Override
    public String getEndpointURL() {
        return gcsServerContainer.url();
    }

    @Override
    public String getURLPathPattern(String bucketName) {
        return String.format("/upload/storage/v1/b/%s/o", bucketName);
    }

    @Override
    public boolean enableProxy(Map<String, String> config, WireMockServer wireMockServer) {
        return false;
    }

    @Override
    public CompressionType getDefaultCompression() {
        return null;
    }

    @Override
    public BucketAccessor<String> getBucketAccessor(String bucketName) {
        return new GCSBucketAccessor(this, bucketName);
    }

    @Override
    public WireMockServer enableFaultyProxy() {
        return null;
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return GcsSinkConnector.class;
    }

    @Override
    public void createStorage() {

    }

    @Override
    public void removeStorage() {

    }

    @Override
    public List<? extends NativeInfo<String, Blob>> getNativeStorage() {
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

    public final class GCSBucketAccessor extends BucketAccessor<String> {

        private final BucketInfo bucketInfo;

        public GCSBucketAccessor(final GCSSinkStorage storage, final String bucketName) {
            super(storage, bucketName);
            bucketInfo =  BucketInfo.of(bucketName);
        }

        private BlobId blobId(final String objectKey) {
            return BlobId.of(bucketInfo.getName(), objectKey);
        }

        private BlobInfo blobInfo(String objectKey) {
            return BlobInfo.newBuilder(bucketInfo, objectKey).build();
        }
        @Override
        protected InputStream getInputStream(String objectKey) throws IOException {
            return new ByteArrayInputStream(storage.readAllBytes(blobId(objectKey)));
        }

        private List<String> extractKeys(Page<Blob> page) {
            return StreamSupport.stream(page.iterateAll().spliterator(), false)
                    .map(BlobInfo::getName)
                    .sorted()
                    .collect(Collectors.toList());
        }

        @Override
        protected List<String> listKeys(String prefix) throws IOException {
            return extractKeys(StringUtils.isEmpty(prefix) ? storage.list(bucketInfo.getName()) : storage.list(bucketInfo.getName(), Storage.BlobListOption.prefix(prefix)));
        }

        @Override
        public void removeBucket() {
            Page<Blob> page = storage.list(bucketInfo.getName());
            page.iterateAll().forEach(blob -> storage.delete(blob.getBlobId()));
            if (!storage.delete(bucketInfo.getName())) {
                throw new IllegalStateException(String.format("Bucket %s was not deleted", bucketInfo.getName()));
            }
        }

        @Override
        public void createBucket() {
            storage.create(bucketInfo);
        }
    }
}
