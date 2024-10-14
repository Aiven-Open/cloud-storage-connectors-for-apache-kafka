package io.aiven.kafka.connect.s3.source.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.aiven.kafka.connect.s3.source.AivenKafkaConnectS3SourceConnector;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.ByteArrayTransformer;
import io.aiven.kafka.connect.s3.source.output.Transformer;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.aiven.kafka.connect.s3.source.S3SourceTask.*;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.*;
import static io.aiven.kafka.connect.s3.source.output.TransformerFactory.DEFAULT_TRANSFORMER_NAME;
import static io.aiven.kafka.connect.s3.source.utils.AivenS3SourceRecordIterator.OFFSET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AivenS3SourceRecordIteratorTest {

    //  public AivenS3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
    //                                       final SourceTaskContext context, final Transformer outputWriter,
    //                                       final Iterator<S3ObjectSummary> s3ObjectSummaryIterator,
    //                                       final Consumer<String> badS3ObjectConsumer) {
    private AivenS3SourceRecordIterator underTest;

    private S3SourceConfig s3SourceConfig;

    private AmazonS3 s3Client;

    private Map<String, S3Object> keyObjectMap = new HashMap<>();
    private static final String BUCKET_NAME = "bucket-name";

    private static final String TOPIC_NAME = "test-topic";


    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context = new SourceTaskContext() {
        @Override
        public OffsetStorageReader offsetStorageReader() {
            return offsetStorageReader;
        }
    };

    private Transformer transformer = new ByteArrayTransformer();

    private List<S3ObjectSummary> s3ObjectSummaries;

    @BeforeEach
    public void setup() {
        Map<String, Object> osrMap = new HashMap<>();
        offsetStorageReader = mock(OffsetStorageReader.class);
        when(offsetStorageReader.offset(any())).thenReturn(osrMap);

        s3SourceConfig = new S3SourceConfig(getBasicProperties());

        s3Client = mock(AmazonS3.class);
        when(s3Client.getObject(anyString(), anyString())).thenAnswer(i -> keyObjectMap.get(i.getArguments()[1]));
        s3ObjectSummaries = new ArrayList<>();
    }

    private Map<String,String> getBasicProperties() {
        Map<String,String> properties = new HashMap<>();
        properties.put(S3SourceConfig.OUTPUT_FORMAT_KEY, DEFAULT_TRANSFORMER_NAME);
        properties.put("name", "test_source_connector");
        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("tasks.max", "1");
        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, TOPIC_NAME);
        properties.put(AWS_S3_BUCKET_NAME_CONFIG, BUCKET_NAME);
        return properties;
    }

    private void configure(String fileName, byte[] contents ) {
        S3Object obj = new S3Object();
        obj.setKey(fileName);
        obj.setObjectContent(new ByteArrayInputStream(contents));
        obj.setBucketName(BUCKET_NAME);
        keyObjectMap.put(fileName, obj);
        S3ObjectSummary objSummary = new S3ObjectSummary();
        objSummary.setSize(contents.length);
        objSummary.setKey(fileName);
        objSummary.setBucketName(BUCKET_NAME);
        s3ObjectSummaries.add(objSummary);
    }

    @Test
    public void testResetOffsetManager() {
        fail("not implemented");
    }


    // compile("(?<topicName>[^/]+?)-" + "(?<partitionId>\\d{5})" + "\\.(?<fileExtension>[^.]+)$"); // ex :
    @Test
    public void testNext() {
        byte[] text = "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        String key = TOPIC_NAME+"-0.fileExt";
        configure(key, text);
        List<String> badObjects = new ArrayList<>();
        underTest = new AivenS3SourceRecordIterator(s3SourceConfig, s3Client, BUCKET_NAME, context, transformer, s3ObjectSummaries.iterator(), badObjects::add);
        assertThat(underTest).hasNext();
        AivenS3SourceRecord actual = underTest.next();
        assertThat(actual.getObjectKey()).isEqualTo(key);
        assertThat(actual.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> aivenPartitionMap = new HashMap<>();
        aivenPartitionMap.put(BUCKET, BUCKET_NAME);
        aivenPartitionMap.put(TOPIC, TOPIC_NAME);
        aivenPartitionMap.put(PARTITION, 0);
        assertThat(actual.getPartitionMap()).isEqualTo(aivenPartitionMap);

        assertThat(actual.getOffsetMap().get(OFFSET_KEY)).isEqualTo(0L);
        assertThat(badObjects).isEmpty();

        assertThat(underTest.hasNext()).isFalse();
    }

    @Test
    public void testMultipleRecords() {
        byte[] text = "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        String key = TOPIC_NAME+"-0-1.fileExt";
        configure(key, text);
        byte[] text2 = "Now is the time for all good people to come to the aid of their country.".getBytes(StandardCharsets.UTF_8);
        String key2 = TOPIC_NAME+"-0.fileExt";
        configure(key2, text2);

        List<String> badObjects = new ArrayList<>();
        underTest = new AivenS3SourceRecordIterator(s3SourceConfig, s3Client, BUCKET_NAME, context, transformer, s3ObjectSummaries.iterator(), badObjects::add);
        assertThat(underTest).hasNext();
        AivenS3SourceRecord actual = underTest.next();
        assertThat(actual.getObjectKey()).isEqualTo(key);
        assertThat(actual.key()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> aivenPartitionMap = new HashMap<>();
        aivenPartitionMap.put(BUCKET, BUCKET_NAME);
        aivenPartitionMap.put(TOPIC, TOPIC_NAME);
        aivenPartitionMap.put(PARTITION, 0);
        assertThat(actual.getPartitionMap()).isEqualTo(aivenPartitionMap);

        assertThat(actual.getOffsetMap().get(OFFSET_KEY)).isEqualTo(0L);
        assertThat(badObjects).isEmpty();

        // read 2nd record
        assertThat(underTest).hasNext();
        actual = underTest.next();
        assertThat(actual.getObjectKey()).isEqualTo(key2);
        assertThat(actual.key()).isEqualTo(key2.getBytes(StandardCharsets.UTF_8));

        aivenPartitionMap = new HashMap<>();
        aivenPartitionMap.put(BUCKET, BUCKET_NAME);
        aivenPartitionMap.put(TOPIC, TOPIC_NAME);
        aivenPartitionMap.put(PARTITION, 1);
        assertThat(actual.getPartitionMap()).isEqualTo(aivenPartitionMap);

        assertThat(actual.getOffsetMap().get(OFFSET_KEY)).isEqualTo(1L);
        assertThat(badObjects).isEmpty();

    }



}
