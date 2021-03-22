/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.connect.common.output.parquet;

import java.util.Collections;
import java.util.Map;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ParquetConfigTest {

    @Test
    void testGenerateParquetConfig() {

        final var origins = Map.of(
                "connect.parquet.aa", "aa",
                "connect.parquet.bb", "bb",
                "connect.parquet.cc", "cc",
                "connect.parquet.avro.schema", "aa"
        );

        final var parquetConfig = new ParquetConfig(origins);
        final var config = parquetConfig.parquetConfiguration();

        assertEquals("aa", config.get("parquet.aa"));
        assertEquals("bb", config.get("parquet.bb"));
        assertEquals("cc", config.get("parquet.cc"));
        assertNull(config.get("parquet.avro.schema"));
    }

    @Test
    void testConvertCompressionTypeToParquetCompressorName() {
        assertEquals(
                CompressionCodecName.UNCOMPRESSED,
                new ParquetConfig(
                        Map.of(AivenCommonConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.NONE.name)
                ).compressionCodecName());
        assertEquals(
                CompressionCodecName.UNCOMPRESSED,
                new ParquetConfig(Collections.emptyMap()).compressionCodecName());
        assertEquals(
                CompressionCodecName.ZSTD,
                new ParquetConfig(
                        Map.of(AivenCommonConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name)
                ).compressionCodecName());
    }

}
