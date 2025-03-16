package io.aiven.kafka.connect.common.source.input;

final public class JsonTestDataFixture {


    public static String getJsonRecs(final int recordCount) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            jsonRecords.append(String.format("{\"key\":\"value%d\"}", i));
            if (i < recordCount) {
                jsonRecords.append("\n"); // NOPMD AppendCharacterWithChar
            }
        }
        return jsonRecords.toString();
    }
}
