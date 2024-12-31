package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPICS;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.OBJECT_KEY;
import static io.aiven.kafka.connect.s3.source.utils.OffsetManager.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
public class AwsIntegrationTest implements IntegrationBase {

    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-AWS-test-";

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();

    private static String s3Prefix;

    private S3Client s3Client;
    private String s3Endpoint;

    private BucketAccessor testBucketAccessor;


    @Override
    public String getS3Prefix() {
        return s3Prefix;
    }

    @Override
    public S3Client getS3Client() {
        return s3Client;
    }

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
    }

    @BeforeEach
    void setupAWS() {
        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    @AfterEach
    void tearDownAWS() {
        testBucketAccessor.removeBucket();
        s3Client.close();
    }

    private Map<String, String> getConfig(final String topics, final int maxTasks) {
        final Map<String, String> config = new HashMap<>();
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, getS3Prefix());
        config.put(TARGET_TOPIC_PARTITIONS, "0,1");
        config.put(TARGET_TOPICS, topics);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", String.valueOf(maxTasks));
        return config;
    }
    /**
     * Test the integration with the Amazon connector
     * @param testInfo
     */
    @Test
    void sourceRecordIteratorTest(final TestInfo testInfo) {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> configData = getConfig(topicName, 1);

        configData.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<String> offsetKeys = new ArrayList<>();
        final List<String> expectedKeys = new ArrayList<>();
        // write 2 objects to s3
        expectedKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00000"));
        expectedKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00000"));
        expectedKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00001"));
        expectedKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00001"));

        // we don't expext the empty one.
        offsetKeys.addAll(expectedKeys);
        offsetKeys.add(writeToS3(topicName, new byte[0], "00003"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        S3SourceConfig s3SourceConfig = new S3SourceConfig(configData);
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offsets(any())).thenReturn(new HashMap<>());

        OffsetManager offsetManager = new OffsetManager(context, s3SourceConfig);

        AWSV2SourceClient sourceClient = new AWSV2SourceClient(s3SourceConfig, new HashSet<>());

        SourceRecordIterator sourceRecordIterator = new  SourceRecordIterator(s3SourceConfig, offsetManager,
                TransformerFactory.getTransformer(InputFormat.BYTES), sourceClient);

        HashSet<String> seenKeys = new HashSet<>();
        while (sourceRecordIterator.hasNext()) {
            S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            String key = OBJECT_KEY + SEPARATOR + s3SourceRecord.getObjectKey();
            assertThat(offsetKeys).contains(key);
            seenKeys.add(key);
        }
        assertThat(seenKeys).containsAll(expectedKeys);
    }
}
