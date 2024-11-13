package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Objects;

public class CompressionFragment extends ConfigFragment {

    static final String GROUP_COMPRESSION = "File Compression";
    static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";


    public CompressionFragment(AbstractConfig cfg) {
        super(cfg);
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

    /**
     * Retrieves the defiend compression type.
     * @return the defined compression type or {@link CompressionType#NONE} if there is no defined compression type.
     */
    public CompressionType getCompressionType() {
        return has(FILE_COMPRESSION_TYPE_CONFIG) ? CompressionType.forName(cfg.getString(FILE_COMPRESSION_TYPE_CONFIG)) : CompressionType.NONE;
    }
}
