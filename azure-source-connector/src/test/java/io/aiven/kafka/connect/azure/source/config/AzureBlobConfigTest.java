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

package io.aiven.kafka.connect.azure.source.config;

import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.blob.BlobServiceAsyncClient;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import static org.assertj.core.api.Assertions.assertThat;

final class AzureBlobConfigTest {
    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        // azure props
        AzureBlobConfigFragment.setter(props)
                .blobEndpoint("example.com")
                .accountKey("ACCOUNT_KEY")
                .accountName("ACCOUNT_NAME")
                .containerName("TheContainer")
                .prefix("Prefix")
                .fetchPageSize(10)
                .retryBackoffInitialDelay(Duration.ofSeconds(1))
                .retryBackoffMaxAttempts(5)
                .retryBackoffMaxDelay(Duration.ofSeconds(30))
                .userAgent("myAgent");

//        SourceConfigFragment.setter(props)
//                .maxPollRecords(50)
//                .targetTopic("testtopic")
//                .distributionType(DistributionType.PARTITION)
//                .ringBufferSize(1024)
//                .errorsTolerance(ErrorsTolerance.NONE);
//
//
//        FileNameFragment.setter(props)
//                .prefixTemplate("prefix-template-")
//                .fileCompression(CompressionType.GZIP)
//                .maxRecordsPerFile(5000)
//                .timestampSource(TimestampSource.Type.EVENT)
//                .timestampTimeZone(ZoneId.of("GMT"));
//
//
//        TransformerFragment.setter(props)
//                .inputFormat(InputFormat.PARQUET)
//                .maxBufferSize(2046)
//                .valueConverterSchemaRegistry("http://converter.schema.registry.example.org")
//                .schemaRegistry("http://schema.registry.example.org");
//
//        OutputFormatFragment.setter(props)
//                .withFormatType(FormatType.CSV)
//                .envelopeEnabled(true)
//                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE)
//                .withOutputFields(OutputFieldType.HEADERS);


        final AzureBlobSourceConfig conf = new AzureBlobSourceConfig(props);
        // azure blob
        assertThat(conf.getConnectionString()).isEqualTo("DefaultEndpointsProtocol=HTTPS;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;BlobEndpoint=HTTPS://example.com/TheContainer;");
        assertThat(conf.getAzureContainerName()).isEqualTo("TheContainer");
        assertThat(conf.getAzurePrefix()).isEqualTo("Prefix");
        assertThat(conf.getAzureFetchPageSize()).isEqualTo(10);
        assertThat(conf.getAzureRetryBackoffInitialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(conf.getAzureRetryBackoffMaxAttempts()).isEqualTo(5);
        assertThat(conf.getAzureRetryBackoffMaxDelay()).isEqualTo(Duration.ofSeconds(30));
        RetryOptions retryOptions = conf.getAzureRetryOptions();
        assertThat(retryOptions).isNotNull();
        BlobServiceAsyncClient client = conf.getAzureServiceAsyncClient();
        assertThat(client).isNotNull();

//        // source config
//        assertThat(conf.getMaxPollRecords()).isEqualTo(50);
//        assertThat(conf.getTargetTopic()).isEqualTo("testtopic");
//        assertThat(conf.getDistributionType()).isEqualTo(DistributionType.PARTITION);
//        assertThat(conf.getRingBufferSize()).isEqualTo(1024);
//        assertThat(conf.getErrorsTolerance()).isEqualTo(ErrorsTolerance.NONE);
//
//        // filename config
//        assertThat(conf.getFilenamePrefixTemplate()).isEqualTo("prefix-template-");
////        assertThat(conf.getFileCompression()).isEqualTo(CompressionType.GZIP);
////        assertThat(conf.getMaxRecordsPerFile()).isEqualTo(5000);
////        assertThat(conf.getTimestampSource()).isEqualTo(TimestampSource.Type.EVENT);
////        assertThat(conf.getTimezone()).isEqualTo(ZoneId.of("GMT"));
//
//        // transformer
//        assertThat(conf.getInputFormat()).isEqualTo(InputFormat.PARQUET);
//        assertThat(conf.getTransformerMaxBufferSize()).isEqualTo(2046);
//        assertThat(conf.getSchemaRegistryUrl()).isEqualTo("http://schema.registry.example.org");
////        assertThat(conf.getConverterSchemaRegistryUrl()).isEqualTo("http://converter.schema.registry.example.org");
//
//        // output
////        assertThat(conf.getOutputFormatType()).isEqualTo(FormatType.CSV);
////        assertThat(conf.isEnvelopeEnabled()).isTrue();
////        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(outputFieldEncodingType.BASE64);
////        assertThat(conf.getOutputFields()).isEqualTo(List.of(new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.BASE64)));

    }
}
