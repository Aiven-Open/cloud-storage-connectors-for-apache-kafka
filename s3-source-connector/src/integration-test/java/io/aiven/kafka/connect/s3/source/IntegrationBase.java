/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.s3.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public interface IntegrationBase {
    String PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA = "plugins/s3-source-connector-for-apache-kafka/";
    String S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST = "s3-source-connector-for-apache-kafka-test-";
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    default AdminClient newAdminClient(final String bootstrapServers) {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(adminClientConfig);
    }

    static void extractConnectorPlugin(Path pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assertThat(distFile).exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    static Path getPluginDir() throws IOException {
        final Path testDir = Files.createTempDirectory(S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST);
        return Files.createDirectories(testDir.resolve(PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA));
    }

    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName();
    }

    static void createTopics(final AdminClient adminClient, final List<String> topicNames)
            throws ExecutionException, InterruptedException {
        final var newTopics = topicNames.stream().map(s -> new NewTopic(s, 4, (short) 1)).collect(Collectors.toList());
        adminClient.createTopics(newTopics).all().get();
    }

    static void waitForRunningContainer(final Container<?> container) {
        await().atMost(Duration.ofMinutes(1)).until(container::isRunning);
    }

    static S3Client createS3Client(final LocalStackContainer localStackContainer) {
        return S3Client.builder()
                .endpointOverride(
                        URI.create(localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(localStackContainer.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    static LocalStackContainer createS3Container() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    /**
     * Finds 2 simultaneously free port for Kafka listeners
     *
     * @return list of 2 ports
     * @throws IOException
     *             when port allocation failure happens
     */
    static List<Integer> getKafkaListenerPorts() throws IOException {
        try (ServerSocket socket = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
            return Arrays.asList(socket.getLocalPort(), socket2.getLocalPort());
        } catch (IOException e) {
            throw new IOException("Failed to allocate port for test", e);
        }
    }

    static List<String> consumeByteMessages(final String topic, final int expectedMessageCount,
            String bootstrapServers) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, ByteArrayDeserializer.class,
                ByteArrayDeserializer.class);
        final List<byte[]> objects = consumeMessages(topic, expectedMessageCount, consumerProperties);
        return objects.stream().map(String::new).collect(Collectors.toList());
    }

    static List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers, final String schemaRegistryUrl) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, StringDeserializer.class,
                KafkaAvroDeserializer.class, schemaRegistryUrl);
        return consumeMessages(topic, expectedMessageCount, consumerProperties);
    }

    static List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, StringDeserializer.class,
                JsonDeserializer.class);
        return consumeMessages(topic, expectedMessageCount, consumerProperties);
    }

    static <K, V> List<V> consumeMessages(final String topic, final int expectedMessageCount,
            final Properties consumerProperties) {
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(topic));

            final List<V> recordValues = new ArrayList<>();
            await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
                final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500L));
                for (final ConsumerRecord<K, V> record : records) {
                    recordValues.add(record.value());
                }
                assertThat(recordValues).hasSize(expectedMessageCount);
            });
            return recordValues;
        }
    }

    static Map<String, Object> consumeOffsetMessages(KafkaConsumer<byte[], byte[]> consumer) throws IOException {
        // Poll messages from the topic
        final Map<String, Object> messages = new HashMap<>();
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            Map<String, Object> offsetRec = OBJECT_MAPPER.readValue(record.value(), new TypeReference<>() { // NOPMD
            });
            messages.putAll(offsetRec);
        }
        return messages;
    }

    static <K, V> Properties getConsumerProperties(String bootstrapServers,
            Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer,
            String schemaRegistryUrl) {
        final Properties props = getConsumerProperties(bootstrapServers, keyDeserializer, valueDeserializer);
        props.put("specific.avro.reader", "false"); // Use GenericRecord instead of specific Avro classes
        props.put("schema.registry.url", schemaRegistryUrl); // URL of the schema registry
        return props;
    }

    static <K, V> Properties getConsumerProperties(String bootstrapServers,
            Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
