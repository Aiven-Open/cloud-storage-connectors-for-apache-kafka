package io.aiven.kafka.connect.gcs;

import io.aiven.kafka.connect.gcs.writer.StreamWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class AsyncRecordStreamHandler {

    private final AsyncGcsSinkTask asyncGcsSinkTask;
    private final String file;
    private final String groupId;
    private final CompletableFuture<AsyncWriteResult> asyncFinish;
    private final Future<?> timeout;
    private final long firstOffset;
    private final StreamWriter streamWriter;
    private final TopicPartition topicPartition;
    private final long createTime;

    private boolean finishing = false;
    private int recordCount = 0;

    public AsyncRecordStreamHandler(AsyncGcsSinkTask asyncGcsSinkTask, final String groupId,
                                    SinkRecord firstElement, StreamWriter streamWriter) {
        this.asyncGcsSinkTask = asyncGcsSinkTask;
        this.groupId = groupId;
        this.topicPartition = new TopicPartition(firstElement.topic(), firstElement.kafkaPartition());
        this.firstOffset = firstElement.kafkaOffset();
        this.streamWriter = streamWriter;
        this.file = asyncGcsSinkTask.recordStreamer().getFilename(firstElement);
        this.timeout = AsyncGcsSinkTask.timed.schedule(() -> asyncGcsSinkTask.nonFullStreamTimeout(this), asyncGcsSinkTask.maxAgeRecordsMs(), TimeUnit.MILLISECONDS);
        this.asyncFinish = new CompletableFuture<>();
        this.createTime = System.currentTimeMillis();
    }

    public int recordCount() {
        return recordCount;
    }

    void addRecord(SinkRecord record) {
        if (record.kafkaPartition() != topicPartition.partition() || !record.topic().equals(topicPartition.topic())) {
            throw new ConnectException("Records from different partitions or topics are not allowed in the same group");
        }
        recordCount ++;
        streamWriter.addRecord(record);
    }

    boolean finishing() {
        return finishing;
    }

    /**
     * requested that the file is written (or that writes complete), and the file is closed, flushed etc
     * This should be non-blocking, and when the write completes the asyncFinish should be completed.
     * This method should only be called once
     *
     * @return
     */
    CompletableFuture<AsyncWriteResult> finishWrite(ForkJoinPool exec){
        if (finishing) {
            throw new IllegalStateException("finishWrite called twice");
        }
        finishing = true;
        timeout.cancel(false);
        asyncFinish.completeAsync(streamWriter::finishWriteAsync, exec);
        return asyncFinish;
    }


    public long firstOffset() {
        return firstOffset;
    }

    public String groupId() {
        return groupId;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public CompletableFuture<AsyncWriteResult> asyncFinish() {
        return asyncFinish;
    }

    public long createTime() {
        return createTime();
    }
}
