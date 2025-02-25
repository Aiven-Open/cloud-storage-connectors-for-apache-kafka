package io.aiven.kafka.connect.s3.source.docs;

import io.aiven.kafka.connect.docs.ConfigDocumentation;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class S3SourceConfigDoc {
    @Test
    public void generate() throws IOException {
        ConfigDocumentation.main(new String[]{"-c", S3SourceConfig.class.getName(), "-f", "TEXT",
                "-o", "src/site/markdown/s3-source-connector/S3SourceConfig.txt" } );

        ConfigDocumentation.main(new String[]{"-c", S3SourceConfig.class.getName(), "-f", "YAML",
                "-o", "src/site/s3-source-connector/S3SourceConfig.yml" } );
    }
}
