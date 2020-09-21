package io.aiven.kafka.connect.common.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class JsonOutputWriterTest {

    private static final List<OutputFieldWriter> writers = new ArrayList<>();
    private static final JsonOutputWriter sut = new JsonOutputWriter(writers);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ByteArrayOutputStream byteStream;

    @BeforeAll
    static void setUpAll() {
        writers.add(new JsonValueWriter());
    }

    @BeforeEach
    void setUp() {
        byteStream = new ByteArrayOutputStream();
    }

    @Test
    void flatOneStringJsonValue() {
        SinkRecord record = createRecord("key0", Schema.STRING_SCHEMA, "value0", 1, 1000L);
        assertRecords(Arrays.asList(record), "[\"value0\"]");
    }

    @Test
    void flatManyStringsJsonValue() {
        SinkRecord record1 = createRecord("key0", Schema.STRING_SCHEMA, "value0", 1, 1000L);
        SinkRecord record2 = createRecord("key1", Schema.STRING_SCHEMA, "value1", 2, 1001L);

        assertRecords(Arrays.asList(record1, record2), "[\"value0\",\"value1\"]");
    }

    @Test
    void structManyJsonValue() {
        Schema level2Schema = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA);
        Schema level1Schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA)
                                                    .field("address", level2Schema);

        Struct struct1 = new Struct(level1Schema).put("name", "John")
                                                 .put("address", new Struct(level2Schema).put("city", "Toronto"));
        Struct struct2 = new Struct(level1Schema).put("name", "Pekka")
                                                 .put("address", new Struct(level2Schema).put("city", "Helsinki"));

        SinkRecord record1 = createRecord("key0", level1Schema, struct1, 1, 1000L);
        SinkRecord record2 = createRecord("key1", level1Schema, struct2, 2, 1001L);

        String expected = "[{\"name\":\"John\",\"address\":{\"city\":\"Toronto\"}},{\"name\":\"Pekka\",\"address\":{\"city\":\"Helsinki\"}}]";
        assertRecords(Arrays.asList(record1, record2), expected);
    }

    private SinkRecord createRecord(String key,
                                    Schema valueSchema,
                                    Object value,
                                    int offset,
                                    long timestamp
                                    ) {
        return new SinkRecord(
                "anyTopic",
                0,
                Schema.STRING_SCHEMA,
                key,
                valueSchema,
                value,
                offset,
                timestamp,
                TimestampType.CREATE_TIME);
    }

    private void assertRecords(List<SinkRecord> records, String expected) {
        try {
            sut.startRecording(byteStream);
            for (int i = 0; i < records.size() - 1; i++) {
                sut.writeRecord(records.get(i), byteStream);
            }
            sut.writeLastRecord(records.get(records.size() - 1), byteStream);
            assertEquals(expected, parse(byteStream.toByteArray()).toString());
        } catch (final Exception e) {
            fail();
        }
    }

    private JsonNode parse(byte[] json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }
}
