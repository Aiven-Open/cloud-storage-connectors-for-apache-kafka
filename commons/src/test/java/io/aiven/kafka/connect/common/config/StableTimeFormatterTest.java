package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.templating.Template;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

class StableTimeFormatterTest {
    private final TimestampSource timestampSource = new TimestampSource() {
        int counter = 10;
        @Override
        public ZonedDateTime time(SinkRecord record) {
            return ZonedDateTime.of(2021, 2, 3, counter++, 4, 5, 0, ZoneOffset.UTC);
        }

        @Override
        public Type type() {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    void testStableTime() {
        Template template = Template.of( "yy-{{timestamp:unit=yyyy}},MM-{{timestamp:unit=MM}},dd-{{timestamp:unit=dd}},HH1-{{timestamp:unit=HH}},HH2-{{timestamp:unit=HH}}");

        final var stableTimeFormatter = new StableTimeFormatter(timestampSource);
        final var currentRecord = new SinkRecord("topic", 0, null, null, null, null, 0);
        var result = template.instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, stableTimeFormatter.apply(currentRecord))
                .render();
        //used to be yy-2021,MM-02,dd-03,HH1-13,HH2-14, if we dont cache the time
        assertEquals("yy-2021,MM-02,dd-03,HH1-10,HH2-10", result);

        var result2 = template.instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, stableTimeFormatter.apply(currentRecord))
                .render();
        //ensure time isnt cached across records
        assertEquals("yy-2021,MM-02,dd-03,HH1-11,HH2-11", result2);
    }

}