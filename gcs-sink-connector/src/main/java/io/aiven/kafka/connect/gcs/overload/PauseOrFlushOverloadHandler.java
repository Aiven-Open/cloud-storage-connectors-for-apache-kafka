package io.aiven.kafka.connect.gcs.overload;

import io.aiven.kafka.connect.gcs.AsyncRecordStreamHandler;
import io.aiven.kafka.connect.gcs.AsyncWriteResult;
import io.aiven.kafka.connect.gcs.Instrumentation;
import io.aiven.kafka.connect.gcs.AsyncGcsSinkTask.StreamSnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PauseOrFlushOverloadHandler implements OverloadHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PauseOrFlushOverloadHandler.class);
    private final static Comparator<StreamSnapshotMetadata> bySize =
            Comparator.<StreamSnapshotMetadata>comparingLong(a -> a.recordCount).reversed();;

    //TODO - make these configurable
    private final int softThreshold = 1000;
    //TODO - make these configurable
    private final int panic = 2000;
    private final Instrumentation instrumentation;
    private final OverloadHandlerActions actions;
    private final Lock writingLock = new ReentrantLock() ;
    private final Condition writingCompleted = writingLock.newCondition();

    public PauseOrFlushOverloadHandler(OverloadHandlerActions actions, Instrumentation instrumentation) {
        this.actions = actions;
        this.instrumentation = instrumentation;
        if (panic <= 0 || softThreshold <= 0 || panic < softThreshold) {
            throw new IllegalArgumentException("Panic ("+panic+") and soft ("+softThreshold+") thresholds must be positive and panic must be greater than soft");
        }
    }

    @Override
    public void addedRecords(int recordCount) {
        final long pending = instrumentation.recordsPending();
        final long writing = instrumentation.recordsWriting();

        if (pending + writing > softThreshold) {
            if (pending + writing > panic) {
                startHardFlush();
            } else if (pending > softThreshold) {
                startSoftFlush();
            }
        }
    }

    /**
     * ensure that there are sufficient records finishing to bring the pending count down below the panic threshold
     */
    private void startHardFlush() {
        LOG.warn("Starting hard flush, openPending: {}, panic :{}", instrumentation.recordsPending(), panic);

        //wait for enough of the flushes to complete to bring us down below the panic threshold
        writingLock.lock();
        try {
            while (instrumentation.recordsPending() + instrumentation.recordsWriting() > panic) {
                try {
                    var woken = writingCompleted.await(10, TimeUnit.SECONDS);
                    LOG.info("During hard flush after sleep, woken: {}, openPending: {}, panic :{}", woken, instrumentation.recordsPending(), panic);
                } catch (InterruptedException e) {
                    LOG.info("Interrupted while waiting for hard flush to complete", e);
                    Thread.currentThread().interrupt();
                    // TODO - not sure if we should return or retry
                    //  if we retry then we need to interrupt at the end if we had one interrupt here
                    //  for the moment we return, and assume that we will probably get a callback if
                    //  we are still overloaded
                    return;
                }
            }
        } finally {
            writingLock.unlock();
        }
        LOG.info("Finished hard flush, openPending: {}, panic :{}", instrumentation.recordsPending(), panic);
    }

    /**
     * ensure that there are sufficient records finishing to bring the pending count down below the soft threshold
     */
    private void startSoftFlush() {
        var open = actions.snapshotOpenStreams();
        //recalculate the pending count, and base all decisions on the snapshot - it makes life easier to reason about
        //but in reality records that time out may be removed for the underlying collection

        //we have openPending as a heap variable so we can modify it in the lambda
        final long[] openPending = {open.stream().mapToLong(r -> r.recordCount).sum()};
        if (openPending[0] > softThreshold) {
            LOG.warn("Starting soft flush, openPending: {}, softThreshold :{}", openPending[0], softThreshold);
            open.sort(bySize);
            open
                    .stream()
                    .takeWhile(r -> openPending[0] > softThreshold)
                    .forEach(stream -> {
                        LOG.info("Starting early async write for stream: {}, recordCount :{}", stream.groupId, stream.recordCount);
                        actions.forceStartAsyncWrite(stream);
                    });
            LOG.info("finished soft flush, openPending: {}, softThreshold :{}", openPending[0], softThreshold);
        }
    }

    @Override
    public void startFlush(AsyncRecordStreamHandler stream) {
        LOG.info("startFlush {} records, total pending: {}", stream.recordCount(), instrumentation.recordsPending());
    }

    @Override
    public void finishedFlush(AsyncRecordStreamHandler stream, AsyncWriteResult result) {
        LOG.info("finishedFlush {} records, total pending: {}", stream.recordCount(), instrumentation.recordsPending());
        writingLock.lock();
        try {
            writingCompleted.signalAll();
        } finally {
            writingLock.unlock();
        }
    }

}
