package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.Storage;
import com.google.common.collect.Streams;
import io.aiven.kafka.connect.common.grouper.RecordStreamer;
import io.aiven.kafka.connect.gcs.overload.OverloadHandler;
import io.aiven.kafka.connect.gcs.overload.OverloadHandlerActions;
import io.aiven.kafka.connect.gcs.overload.PauseOrFlushOverloadHandler;
import io.aiven.kafka.connect.gcs.writer.LateWriteStreamWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AsyncGcsSinkTask extends GcsSinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncGcsSinkTask.class);
    final static ForkJoinPool exec = ForkJoinPool.commonPool();
    final static ScheduledExecutorService timed = Executors.newSingleThreadScheduledExecutor();

    private final ConcurrentMap<String, AsyncRecordStreamHandler> openStreams = new ConcurrentHashMap<>();
    private final Set<AsyncRecordStreamHandler> finishing = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicReference<ConnectException> fatalError = new AtomicReference<>();
    private final Instrumentation instrumentation = new Instrumentation();

    //config
    private AsyncGcsSinkConfig config;
    private int maxRecordsPerFile;
    private int maxAgeRecordsMs;
    private RecordStreamer recordStreamer;
    private OverloadHandler overloadHandler;

    // required by Connect
    public AsyncGcsSinkTask() {
        super();
    }

    // for testing
    public AsyncGcsSinkTask(final Map<String, String> props, final Storage storage) {
        super(props, storage);
    }

    protected void setConfig(AsyncGcsSinkConfig config) {
        this.config = config;
        super.setConfig(config);
    }

    @Override
    protected void parseConfig(Map<String, String> props) {
        setConfig(new AsyncGcsSinkConfig(props));
    }

    @Override
    public void start(final Map<String, String> props) {
        super.start(props);
        this.maxRecordsPerFile = config.getMaxRecordsPerFile();
        this.maxAgeRecordsMs = config.getAsyncMaxRecordAgeMs();
        if (recordGrouper instanceof RecordStreamer) {
            recordStreamer = (RecordStreamer) recordGrouper;
        } else {
            throw new ConnectException("RecordGrouper must implement RecordStreamer to use the async sink task");
        }
        //TODO drive this through config
        this.overloadHandler = new PauseOrFlushOverloadHandler(new OverloadActions(), instrumentation);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        checkForErrors();
        Objects.requireNonNull(records, "records cannot be null");

        LOG.debug("Processing {} records", records.size());

        for (final SinkRecord record : records) {
            var groupId = recordStreamer.getStream(record);
            openStreams.compute(groupId, (group, stream) -> {
                if (stream == null) {
                    // set the value to the new stream, with first element set
                    return createRecordStream(group, record);
                } else {
                    stream.addRecord(record);
                    if (stream.recordCount() >= maxRecordsPerFile) {
                        startAsyncWrite(stream);
                        //and remove the entry from the map
                        return null;
                    } else {
                        //keep the entry in the map
                        return stream;
                    }
                }
            });
        }
        instrumentation.addedRecords(records.size());
        overloadHandler.addedRecords(records.size());
    }

    private void checkForErrors() {
        ConnectException error = fatalError.get();
        if (error != null) {
            throw error;
        }
    }
    public void fatalError(ConnectException error) {
        if (fatalError.compareAndSet(null, error)){
            LOG.error("Fatal error", error);
        } else {
            LOG.error("Another fatal error - which is suppressed as another fatal error has already been reported", error);
        }
        throw error;
    }

    private AsyncRecordStreamHandler createRecordStream(String group, SinkRecord record) {
        return new AsyncRecordStreamHandler(this, group, record,
                //TODO drive the early/late write through config
                new LateWriteStreamWriter(writeHelper(), recordStreamer.getFilename(record)));
    }


    void nonFullStreamTimeout(AsyncRecordStreamHandler streamTimedOut) {
        openStreams.compute(streamTimedOut.groupId(), (group, stream) -> {
            if (stream != streamTimedOut) {
                // if stream is null, streamTimedOut became full and was removed
                // if stream is a different object, streamTimedOut became full and then another record was added to the same groupId
                // either way streamTimedOut should have been started
                if (!streamTimedOut.finishing()) {
                    fatalError(new ConnectException("Stream not started, but not in  - program error"));
                }
                return stream;
            } else {
                //start the async write, and remove it from the map
                startAsyncWrite(streamTimedOut);
                return null;
            }
        });
    }

    /**
     * Start the async write for the stream. Guaranteed to not be called twice for the same stream
     * @implNote guaranteed not to be called concurrently, only called via a
     * {@link ConcurrentHashMap#compute(Object, BiFunction) compute} method
     * of a {@link ConcurrentHashMap}, which enforces memory barriers.
     */
    private void startAsyncWrite(AsyncRecordStreamHandler stream) {
        finishing.add(stream);
        instrumentation.startFlush(stream);
        overloadHandler.startFlush(stream);
        stream.finishWrite(exec)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        fatalError(new ConnectException("Error writing records", error));
                    }
                    //fatalError always throws, so this is success handling
                    finishing.remove(stream);
                    instrumentation.finishedFlush(stream, result);
                    overloadHandler.finishedFlush(stream, result);
                });
    }


    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        checkForErrors();
        commitPending.set(false);
        //we need to hold the 'lock' on the value, for startAsyncWrite to work
        openStreams.forEach((k,v) -> openStreams.compute(k, (group, stream) -> {
            startAsyncWrite(stream);
            return null;
        }));

        CompletableFuture<?>[] list = finishing.stream().map(AsyncRecordStreamHandler::asyncFinish).toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(list).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ConnectException(e);
        }
        finishing.clear();

    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        checkForErrors();
        // we need to check the openStreams and the completing
        // in that order to ensure that we don't miss anything
        //so check the openStream, and then the finishing, as they may move asynchronously from one to the other!

        final Map<TopicPartition, Integer> earliestStillPending = new HashMap<>();
        Streams.concat(openStreams.values().stream(),
                        finishing.stream())
                .forEach(pendingStreamHandler ->
                        earliestStillPending.compute(pendingStreamHandler.topicPartition(),
                                (ignored, earliestPendingOffset) -> {
                                    if (earliestPendingOffset == null || pendingStreamHandler.firstOffset() < earliestPendingOffset) {
                                        return (int) pendingStreamHandler.firstOffset();
                                    } else {
                                        return earliestPendingOffset;
                                    }
                                })
                );

        return currentOffsets
                .entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                suppliedEntry -> {
                                    var inTransit = earliestStillPending.get(suppliedEntry.getKey());
                                    if (inTransit == null) {
                                        return suppliedEntry.getValue();
                                    } else {
                                        return new OffsetAndMetadata(inTransit, suppliedEntry.getValue().metadata());
                                    }
                                }));
    }


    private final AtomicBoolean commitPending = new AtomicBoolean(false);
    public void requestCommitNow() {
        context.requestCommit();
    }
    public void requestCommitSoon() {
        if (commitPending.compareAndSet(false, true)) {
            //TODO config
            timed.schedule(this::requestCommitNow, 100, TimeUnit.MILLISECONDS);
        }
    }

    public RecordStreamer recordStreamer() {
        return recordStreamer;
    }

    public long maxAgeRecordsMs() {
        return maxAgeRecordsMs;
    }

    private class OverloadActions implements OverloadHandlerActions {
        @Override
        public List<StreamSnapshotMetadata> snapshotOpenStreams() {
            return openStreams.values().stream().map(StreamSnapshotMetadata::new).collect(Collectors.toList());
        }

        @Override
        public void forceStartAsyncWrite(StreamSnapshotMetadata streamSnapshotMetadata) {
            openStreams.compute(streamSnapshotMetadata.groupId, (group, stream) -> {
                //we dont really care if the stream is null. that means that the openStreams has been updated
                //and its already enqueued for writing, e.g. because a timer has expired
                if (stream != null) {
                    startAsyncWrite(stream);
                }
                return null;
            });
        }
    }
    //TODO should be a record
    public static class StreamSnapshotMetadata {
        public final long createTime;
        public final int recordCount;
        public final long firstOffset;
        public final String groupId;
        public final TopicPartition file;
        public final boolean finishing;

        public StreamSnapshotMetadata(AsyncRecordStreamHandler stream) {
            this.createTime = stream.createTime();
            this.recordCount = stream.recordCount();
            this.firstOffset = stream.firstOffset();
            this.groupId = stream.groupId();
            this.file = stream.topicPartition();
            this.finishing = stream.finishing();
        }

    }
}
