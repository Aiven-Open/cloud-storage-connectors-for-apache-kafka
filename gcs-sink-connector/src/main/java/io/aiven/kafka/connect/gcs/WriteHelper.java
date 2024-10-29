package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.aiven.kafka.connect.common.output.OutputWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.channels.Channels;
import java.util.List;

public class WriteHelper {
    private final Storage storage;
    private final GcsSinkConfig config;

    public WriteHelper(Storage storage, GcsSinkConfig config) {
        this.storage = storage;
        this.config = config;
    }

    public OutputWriter openFile(final String filename) {
        final BlobInfo blob = BlobInfo.newBuilder(config.getBucketName(), config.getPrefix() + filename)
                .setContentEncoding(config.getObjectContentEncoding())
                .build();
        try {
            var out = Channels.newOutputStream(storage.writer(blob));
            return OutputWriter.builder()
                    .withExternalProperties(config.originalsStrings())
                    .withOutputFields(config.getOutputFields())
                    .withCompressionType(config.getCompressionType())
                    .withEnvelopeEnabled(config.envelopeEnabled())
                    .build(out, config.getFormatType());
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException(e);
        }
    }
    public void flushFile(final String filename, final List<SinkRecord> records) {
        try (var writer = openFile(filename)) {
            writer.writeRecords(records);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException(e);
        }
    }

}
