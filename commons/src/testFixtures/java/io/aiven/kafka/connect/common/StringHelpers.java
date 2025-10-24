package io.aiven.kafka.connect.common;

public class StringHelpers {
    private StringHelpers() {
        // do not instantiate.
    }
    public static final String quoted(String s) {
        return "\"" + s + "\"";
    }
}
