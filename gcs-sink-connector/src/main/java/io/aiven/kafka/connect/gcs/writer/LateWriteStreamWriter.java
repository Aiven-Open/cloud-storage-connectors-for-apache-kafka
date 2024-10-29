package io.aiven.kafka.connect.gcs.writer;

import io.aiven.kafka.connect.gcs.AsyncWriteResult;
import io.aiven.kafka.connect.gcs.WriteHelper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class LateWriteStreamWriter extends StreamWriter {
    private final List<SinkRecord> records;
    private final WriteHelper writeHelper;
    private final String filename;

    public LateWriteStreamWriter(WriteHelper writeHelper, String filename) {
        this.writeHelper = writeHelper;
        this.filename = filename;
        this.records = new ArrayList<>();
    }

    @Override
    public void addRecord(SinkRecord record) {
        records.add(record);
    }

    @Override
    public AsyncWriteResult finishWriteAsync() {
        final var start = System.nanoTime();
        writeHelper.flushFile(filename, records);
        return new AsyncWriteResult(records.size(), 0, System.currentTimeMillis() - start);
    }


}
