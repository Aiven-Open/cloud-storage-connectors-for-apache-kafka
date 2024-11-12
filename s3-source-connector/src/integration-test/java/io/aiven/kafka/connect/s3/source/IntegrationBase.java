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

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public interface IntegrationBase {
    String PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA = "plugins/s3-source-connector-for-apache-kafka/";
    String S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST = "s3-source-connector-for-apache-kafka-test-";
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    default AdminClient newAdminClient(final String bootstrapServers) {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(adminClientConfig);
    }

    static void extractConnectorPlugin(File pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    static File getPluginDir() throws IOException {
        final File testDir = Files.createTempDirectory(S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST).toFile();

        final File pluginDir = new File(testDir, PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA);
        assert pluginDir.mkdirs();
        return pluginDir;
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

    static AmazonS3 createS3Client(final LocalStackContainer localStackContainer) {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    static LocalStackContainer createS3Container() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    static int getRandomPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IOException("Failed to allocate port for test", e);
        }
    }

    static List<String> consumeMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final List<String> messages = new ArrayList<>();

            // Poll messages from the topic
            while (messages.size() < expectedMessageCount) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(5L);
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    messages.add(new String(record.value(), StandardCharsets.UTF_8));
                }
            }

            return messages;
        }
    }

    static List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers, final String schemaRegistryUrl) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-avro");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Assuming string
                                                                                                     // key
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName()); // Avro
                                                                                                          // deserializer
                                                                                                          // for values
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", schemaRegistryUrl); // URL of the schema registry
        props.put("specific.avro.reader", "false"); // Use GenericRecord instead of specific Avro classes

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final List<GenericRecord> recordsList = new ArrayList<>();

            // Poll messages from the topic
            while (recordsList.size() < expectedMessageCount) {
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(500L);
                for (final ConsumerRecord<String, GenericRecord> record : records) {
                    recordsList.add(record.value());
                }
            }

            return recordsList;
        }
    }

    static List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-json");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Assuming string
        // key
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()); // Json
        // deserializer
        // for values
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            final List<JsonNode> recordsList = new ArrayList<>();

            // Poll messages from the topic
            while (recordsList.size() < expectedMessageCount) {
                final ConsumerRecords<String, JsonNode> records = consumer.poll(500L);
                for (final ConsumerRecord<String, JsonNode> record : records) {
                    recordsList.add(record.value()); // Add the GenericRecord to the list
                }
            }

            return recordsList;
        }
    }

    static Map<String, Object> consumeOffsetStorageMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServer) throws ConnectException {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final Map<String, Object> messages = new HashMap<>();
            consumer.subscribe(Collections.singletonList(topic));

            // Poll messages from the topic
            while (messages.size() < expectedMessageCount) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(5L);
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    messages.putAll(OBJECT_MAPPER.readValue(new String(record.value(), StandardCharsets.UTF_8), // NOPMD
                            new TypeReference<>() { // NOPMD
                            }));
                }
            }
            return messages;

        } catch (JsonProcessingException e) {
            throw new ConnectException("Error while consuming messages " + e.getMessage());
        }
    }
}
