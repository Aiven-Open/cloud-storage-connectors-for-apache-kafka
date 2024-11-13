package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Objects;

public class CompressionFragment {

    private static final String GROUP_COMPRESSION = "File Compression";
    private static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";

    private AbstractConfig cfg;

    public CompressionFragment(AbstractConfig cfg) {
        this.cfg = cfg;
    }

    public static ConfigDef update(final ConfigDef configDef,  final CompressionType defaultCompressionType) {
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
                Objects.isNull(defaultCompressionType) ? null : defaultCompressionType.name, // NOPMD NullAssignment
                new FileCompressionTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on GCS. " + "The supported values are: "
                        + CompressionType.SUPPORTED_COMPRESSION_TYPES + ".",
                GROUP_COMPRESSION, 1, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));
        return configDef;
    }

    public CompressionType getCompressionType() {
        return CompressionType.forName(cfg.getString(FILE_COMPRESSION_TYPE_CONFIG));
    }
}
