package io.aiven.kafka.connect.gcs.overload;

import io.aiven.kafka.connect.gcs.AsyncGcsSinkTask;
import io.aiven.kafka.connect.gcs.AsyncRecordStreamHandler;
import io.aiven.kafka.connect.gcs.AsyncWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public interface OverloadHandler {
    void addedRecords(int recordCount);
    void startFlush(AsyncRecordStreamHandler stream);
    void finishedFlush(AsyncRecordStreamHandler stream, AsyncWriteResult result);
}
