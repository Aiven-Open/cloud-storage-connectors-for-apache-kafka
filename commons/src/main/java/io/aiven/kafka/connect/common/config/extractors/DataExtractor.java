package io.aiven.kafka.connect.common.config.extractors;

import org.apache.kafka.connect.sink.SinkRecord;

public interface DataExtractor {

    Object extractDataFrom(final SinkRecord record);
}
