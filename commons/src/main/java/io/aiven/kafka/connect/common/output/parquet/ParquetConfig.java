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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

final class ParquetConfig extends AbstractConfig {

    public ParquetConfig(final Map<?, ?> originals) {
        super(new ConfigDef(), originals);
    }

    public Configuration parquetConfiguration() {
        final var config = new Configuration();
        for (final var e : originalsWithPrefix("connect.").entrySet()) {
            if (!e.getKey().startsWith("parquet")) {
                continue;
            }
            // ParquetSchemaBuilder builds schema explicitly for ParquetWriter,
            // parquet.avro.schema property is excluded, all others are accepted
            if ("parquet.avro.schema".equals(e.getKey())) {
                continue;
            }
            config.set(e.getKey(), e.getValue().toString());
        }
        return config;
    }

    public CompressionCodecName compressionCodecName() {
        final var connectorCompressionType = CompressionType.forName(
                originals().getOrDefault(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.NONE.name)
                        .toString());
        switch (connectorCompressionType) {
            case GZIP :
                return CompressionCodecName.GZIP;
            case SNAPPY :
                return CompressionCodecName.SNAPPY;
            case ZSTD :
                return CompressionCodecName.ZSTD;
            default :
                return CompressionCodecName.UNCOMPRESSED;
        }
    }

}
