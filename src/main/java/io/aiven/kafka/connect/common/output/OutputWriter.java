package io.aiven.kafka.connect.common.output;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputWriter {

    void writeLastRecord(final SinkRecord record,
                         final OutputStream outputStream) throws IOException;

    void writeRecord(final SinkRecord record,
                     final OutputStream outputStream) throws IOException;
}
