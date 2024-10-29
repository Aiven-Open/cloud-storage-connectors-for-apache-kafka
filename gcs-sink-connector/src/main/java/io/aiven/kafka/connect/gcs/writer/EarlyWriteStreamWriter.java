package io.aiven.kafka.connect.gcs.writer;

import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.gcs.AsyncWriteResult;
import io.aiven.kafka.connect.gcs.WriteHelper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

public class EarlyWriteStreamWriter extends StreamWriter {
    private final OutputWriter writer;
    private int recordCount;
    private long durationNs;

    public EarlyWriteStreamWriter(WriteHelper writeHelper, String filename) {
        this.writer = writeHelper.openFile(filename);
    }

    @Override
    public void addRecord(SinkRecord record) {
        final var start = System.nanoTime();
        try {
            writer.writeRecord(record);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
        durationNs += System.nanoTime() - start;
        recordCount++;
    }

    @Override
    public AsyncWriteResult finishWriteAsync() {
        final var start = System.nanoTime();
        try {
            writer.close();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
        return new AsyncWriteResult(recordCount, durationNs, System.nanoTime() - start);
    }


}
