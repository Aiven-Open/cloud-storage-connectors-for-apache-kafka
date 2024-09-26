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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.dockerjava.api.model.Ulimit;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public interface IntegrationBase {

    String DOCKER_IMAGE_KAFKA = "confluentinc/cp-kafka:7.7.0";

    default AdminClient newAdminClient(final KafkaContainer kafka) {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    default ConnectRunner newConnectRunner(final KafkaContainer kafka, final File pluginDir,
            final int offsetFlushIntervalMs) {
        return new ConnectRunner(pluginDir, kafka.getBootstrapServers(), offsetFlushIntervalMs);
    }

    static void extractConnectorPlugin(File pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    static File getPluginDir() throws IOException {
        final File testDir = Files.createTempDirectory("s3-source-connector-for-apache-kafka-test-").toFile();

        final File pluginDir = new File(testDir, "plugins/s3-source-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();
        return pluginDir;
    }

    static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse(DOCKER_IMAGE_KAFKA))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withNetwork(Network.newNetwork())
                .withExposedPorts(KafkaContainer.KAFKA_PORT, 9092)
                .withCreateContainerCmdModifier(
                        cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))));
    }

    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName();// + "-" + testInfo.getDisplayName().hashCode();
    }

    static void createTopics(final AdminClient adminClient, final List<String> topicNames)
            throws ExecutionException, InterruptedException {
        final var newTopics = topicNames.stream().map(s -> new NewTopic(s, 4, (short) 1)).collect(Collectors.toList());
        adminClient.createTopics(newTopics).all().get();
    }

    static void waitForRunningContainer(final Container<?> kafka) {
        Awaitility.await().atMost(Duration.ofMinutes(1)).until(kafka::isRunning);
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

    static List<String> consumeMessages(final String topic, final int expectedMessageCount,
            final KafkaContainer kafka) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
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
            final KafkaContainer kafka, final String schemaRegistryUrl) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
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
                    recordsList.add(record.value()); // Add the GenericRecord to the list
                }
            }

            return recordsList;
        }
    }
}
