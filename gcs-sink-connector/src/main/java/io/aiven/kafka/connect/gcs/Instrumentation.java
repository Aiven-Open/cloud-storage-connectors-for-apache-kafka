package io.aiven.kafka.connect.gcs;

import java.util.concurrent.atomic.AtomicLong;

public class Instrumentation {
    private final AtomicLong recordsTotalReceived = new AtomicLong();
    private final AtomicLong recordsTotalCompleted = new AtomicLong();

    // received, but not started flush
    private final AtomicLong recordsPending = new AtomicLong();
    //in the process of flushing
    private final AtomicLong recordsWriting = new AtomicLong();

    private final AtomicLong filesStarted = new AtomicLong();
    private final AtomicLong filesCompleted = new AtomicLong();

    private final AtomicLong writeDurationNs = new AtomicLong();
    private final AtomicLong closeDurationNs = new AtomicLong();

    void addedRecords(int recordCount) {
        recordsTotalReceived.addAndGet(recordCount);
        recordsPending.addAndGet(recordCount);
    }

    void finishedFlush(AsyncRecordStreamHandler stream, AsyncWriteResult result) {
        filesCompleted.incrementAndGet();

        recordsTotalCompleted.addAndGet(stream.recordCount());

        recordsWriting.addAndGet(-stream.recordCount());

        writeDurationNs.addAndGet(result.writeDurationNs());
        closeDurationNs.addAndGet(result.closeDurationNs());
    }

    void startFlush(AsyncRecordStreamHandler stream) {
        filesStarted.incrementAndGet();
        recordsPending.addAndGet(-stream.recordCount());
        recordsWriting.addAndGet(stream.recordCount());
    }

    public long recordsTotalReceived() {
        return recordsTotalReceived.get();
    }
    public long recordsTotalCompleted() {
        return recordsTotalCompleted.get();
    }

    public long recordsPending() {
        return recordsPending.get();
    }
    public long recordsBuffered() {
        return recordsPending.get() + recordsWriting.get();
    }

    public long recordsWriting() {
        return recordsWriting.get();
    }


    public long filesStarted() {
        return filesStarted.get();
    }

    public long filesCompleted() {
        return filesCompleted.get();
    }

    public long writeDurationNs() {
        return writeDurationNs.get();
    }

    public long closeDurationNs() {
        return closeDurationNs.get();
    }

}
