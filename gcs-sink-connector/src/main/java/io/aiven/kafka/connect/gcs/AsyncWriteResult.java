package io.aiven.kafka.connect.gcs;

public class AsyncWriteResult {
    private final int recordCount;
    private final long writeDurationNs;
    private final long closeDurationNs;

    public AsyncWriteResult(int recordCount, long writeDurationNs, long closeDurationNs) {
        this.recordCount = recordCount;
        this.writeDurationNs = writeDurationNs;
        this.closeDurationNs = closeDurationNs;
    }

    public int recordCount() {
        return recordCount;
    }

    public long writeDurationNs() {
        return writeDurationNs;
    }

    public long closeDurationNs() {
        return closeDurationNs;
    }
}
