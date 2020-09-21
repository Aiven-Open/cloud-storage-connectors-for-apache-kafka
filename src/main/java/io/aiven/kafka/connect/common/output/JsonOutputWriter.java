package io.aiven.kafka.connect.common.output;

import io.aiven.kafka.connect.common.config.OutputField;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JsonOutputWriter implements OutputWriter {

    private static final byte[] BATCH_START = "[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BATCH_END = "]".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);

    private final List<OutputFieldWriter> writers;

    JsonOutputWriter(final List<OutputFieldWriter> writers) {
        this.writers = writers;
    }

    public void startRecording(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        outputStream.write(BATCH_START);
    }

    public void writeRecord(final SinkRecord record,
                            final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        writeFields(record, outputStream);
        outputStream.write(RECORD_SEPARATOR);
    }

    public void writeLastRecord(final SinkRecord record,
                                final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        writeFields(record, outputStream);
        outputStream.write(BATCH_END);
    }

    private void writeFields(final SinkRecord record,
                             final OutputStream outputStream) throws IOException {
        final Iterator<OutputFieldWriter> writerIter = writers.iterator();
        writerIter.next().write(record, outputStream);
        while (writerIter.hasNext()) {
            outputStream.write(FIELD_SEPARATOR);
            writerIter.next().write(record, outputStream);
        }
    }
}
