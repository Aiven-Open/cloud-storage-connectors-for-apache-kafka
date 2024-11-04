package io.aiven.kafka.connect.gcs.overload;

import io.aiven.kafka.connect.gcs.AsyncGcsSinkTask.StreamSnapshotMetadata;
import java.util.List;

/**
 * The limited actions that the overload handler can take
 */
public interface OverloadHandlerActions {
    /**
     * Get the list of open streams. Note - the list is a snapshot and may not be up to date. Streams may be removed
     * asynchronously as they time out. No new streams will be added, or records added to existing streams by background threads
     *
     * @return the list of open streams
     */
    List<StreamSnapshotMetadata> snapshotOpenStreams();

    /**
     * force a stream to start writing asynchronously or synchronously (depending on the writer implementation)
     * Typically used to retrieve memory pressure
     *
     * @param streamSnapshotMetadata the stream to force write
     */
    void forceStartAsyncWrite(StreamSnapshotMetadata streamSnapshotMetadata);
}
