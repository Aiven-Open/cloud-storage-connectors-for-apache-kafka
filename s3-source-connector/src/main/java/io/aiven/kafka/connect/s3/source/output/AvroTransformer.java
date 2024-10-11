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

package io.aiven.kafka.connect.s3.source.output;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.util.IOUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the transform from the S3 input stream Avro based data for the @{code byte[]} found in a {@code ConsumerRecord<byte[], byte[]>}.
 */
public class AvroTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTransformer.class);

    @Override
    public String getName() {
        return "avro";
    }

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public Iterator<byte[]> byteArrayIterator(InputStream inputStream, String topic, S3SourceConfig s3SourceConfig) throws BadDataException {
                final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (SeekableByteArrayInput sin = new SeekableByteArrayInput(IOUtils.toByteArray(inputStream));
             DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader);
             KafkaAvroSerializer avroSerializer = createAvroSerializer(s3SourceConfig)) {
            List<byte[]> result = new ArrayList<>();
            for (GenericRecord genericRecord : reader) {
                result.add(avroSerializer.serialize(topic, genericRecord));
            }
            return result.iterator();
        } catch (IOException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new BadDataException(e);
        }
    }

    /**
     * Creates an fully configured KafkaAvroSerializer to create the output {@code byte[]}.
     * Uses the {@link S3SourceConfig#SCHEMA_REGISTRY_URL} and {@link S3SourceConfig#VALUE_SERIALIZER} values from the configuration to configure the serializer.
     * @param s3SourceConfig The S3 source configuration.
     * @return a fully configured KafkaAvroSerializer.
     * @throws NoSuchMethodException if the value serializer class does not define a no-argument constructor.
     * @throws InvocationTargetException if the value serializer class can not be instantiated.
     * @throws InstantiationException if the value serializer class can not be instantiated.
     * @throws IllegalAccessException if the value serializer class constructor can not be accessed.
     */
    static KafkaAvroSerializer createAvroSerializer(S3SourceConfig s3SourceConfig) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        final Map<String, String> config = Collections.singletonMap(SCHEMA_REGISTRY_URL,
                s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
        KafkaAvroSerializer avroSerializer = (KafkaAvroSerializer) s3SourceConfig.getClass(VALUE_SERIALIZER)
                .getDeclaredConstructor().newInstance();
        avroSerializer.configure(config, false);
        return avroSerializer;
    }

}
