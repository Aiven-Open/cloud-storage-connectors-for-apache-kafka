package io.aiven.kafka.connect.gcs.writer;

import io.aiven.kafka.connect.gcs.AsyncWriteResult;
import org.apache.kafka.connect.sink.SinkRecord;

public abstract class StreamWriter {

    public abstract void addRecord(SinkRecord record);

    public abstract AsyncWriteResult finishWriteAsync();
}
