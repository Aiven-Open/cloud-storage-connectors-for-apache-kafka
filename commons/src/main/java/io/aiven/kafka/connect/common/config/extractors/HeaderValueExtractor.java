package io.aiven.kafka.connect.common.config.extractors;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class HeaderValueExtractor implements DataExtractor {
    private final String headerKey;

    public HeaderValueExtractor(final String headerKey) {
        this.headerKey = headerKey;
    }

    public Object extractDataFrom(final SinkRecord record) {
        final Header header = record.headers().lastWithName(headerKey);
        return header == null ? null : header.value();
    }
}
