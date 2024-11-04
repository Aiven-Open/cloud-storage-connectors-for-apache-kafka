/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.gcs;

import java.nio.channels.Channels;
import java.util.*;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkConnector.class);
    private static final String USER_AGENT_HEADER_KEY = "user-agent";

    protected RecordGrouper recordGrouper;

    private GcsSinkConfig config;

    private Storage storage;
    private WriteHelper writeHelper;


    // required by Connect
    public GcsSinkTask() {
        super();
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props, final Storage storage) {
        super();

        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(storage, "storage cannot be null");

        parseConfig(props);
        this.storage = storage;
        initRest();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        parseConfig(props);
        StorageOptions.Builder builder = StorageOptions.newBuilder()
                .setHost(config.getGcsEndpoint())
                .setCredentials(config.getCredentials())
                .setHeaderProvider(FixedHeaderProvider.create(USER_AGENT_HEADER_KEY, config.getUserAgent()))
                .setRetrySettings(RetrySettings.newBuilder()
                        .setInitialRetryDelay(config.getGcsRetryBackoffInitialDelay())
                        .setMaxRetryDelay(config.getGcsRetryBackoffMaxDelay())
                        .setRetryDelayMultiplier(config.getGcsRetryBackoffDelayMultiplier())
                        .setTotalTimeout(config.getGcsRetryBackoffTotalTimeout())
                        .setMaxAttempts(config.getGcsRetryBackoffMaxAttempts())
                        .build());
        if (config.getApiTracerFactory() != null) {
            builder.setApiTracerFactory(config.getApiTracerFactory());
        }
        this.storage = builder.build()
                .getService();
        initRest();
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
        this.writeHelper = new WriteHelper(storage, config);
    }

    protected void setConfig(GcsSinkConfig gcsSinkConfig) {
        this.config = gcsSinkConfig;
    }

    protected void parseConfig(Map<String, String> props) {
        setConfig(new GcsSinkConfig(props));
    }

    private void initRest() {
        try {
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD broad exception catched
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        LOG.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            final Stream<Map.Entry<String, List<SinkRecord>>> stream;
            if (config.isWriteParallel()) {
                stream = recordGrouper.records().entrySet().stream().parallel();
            } else {
                stream = recordGrouper.records().entrySet().stream();
            }
            stream.forEach(entry -> writeHelper.flushFile(entry.getKey(), entry.getValue()));
        } finally {
            recordGrouper.clear();
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
    protected WriteHelper writeHelper() {
        return writeHelper;
    }

}
