package io.aiven.kafka.connect.s3.source.output;

public class BadDataException extends Exception {
    public BadDataException() {
    }

    public BadDataException(String message) {
        super(message);
    }

    public BadDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadDataException(Throwable cause) {
        super(cause);
    }
}
