package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;

public class StableTimeFormatter {
    private static final Map<String, DateTimeFormatter> TIMESTAMP_FORMATTERS = Map.of("yyyy",
            DateTimeFormatter.ofPattern("yyyy"), "MM", DateTimeFormatter.ofPattern("MM"), "dd",
            DateTimeFormatter.ofPattern("dd"), "HH", DateTimeFormatter.ofPattern("HH"));

    private final Function<SinkRecord, Function<VariableTemplatePart.Parameter, String>> formatter;

    public StableTimeFormatter(TimestampSource timestampSource) {
        this.formatter = record -> {
            var time =  timestampSource.time(record);
            return parameter -> time
                    .format(TIMESTAMP_FORMATTERS.get(parameter.getValue()));
        };
    }
    public Function<VariableTemplatePart.Parameter, String> apply(SinkRecord record) {
        return formatter.apply(record);
    }
}
