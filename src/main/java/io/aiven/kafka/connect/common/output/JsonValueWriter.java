package io.aiven.kafka.connect.common.output;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.json.JsonConverter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

public class JsonValueWriter implements OutputFieldWriter {

    /**
     * Takes the {@link SinkRecord}'s value as a JSON.
     *
     * @param record        the record to get the value from
     * @param outputStream  the stream to write to
     * @throws DataException when the value is not actually a JSON
     */

    private final JsonConverter converter;

    public JsonValueWriter() {
        converter = new JsonConverter();
        // TODO: make a more generic configuration
        converter.configure(Map.of("schemas.enable", false, "converter.type", "value"));
    }

    @Override
    public void write(SinkRecord record, OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(record.valueSchema(), "value schema cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");
        // Do not use Byte value???

        if (record.value() == null) {
            return;
        }
        byte[] bValue = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        outputStream.write(bValue);
    }
}
