package io.aiven.kafka.connect.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Logs the state change of an item.
 * Will create a log entry when the {@link #lastState} object {@code equals()} method returns false when compared to the
 * state object returned from the {@link #stateSupplier}.
 * @param <T> the type of object to check.
 */
public class StateChangeLogger<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateChangeLogger.class);
    /**
     * TRhe name for this logger.  Used in log messages.
     */
    private final Supplier<String> msgSupplier;
    /**
     * The supplier of the current state.
     */
    private final Supplier<T> stateSupplier;
    /**
     * The last state detected.
     */
    private T lastState;

    /**
     * Constructor.
     * @param name the name of the logger.
     * @param supplier the supplier of current state.
     */
    public StateChangeLogger(final String name, final Supplier<T> supplier) {
        this( () -> name, supplier);
    }

    /**
     * Constructor.
     * @param msgSupplier the supplier of messages for the logging.
     * @param supplier the supplier of current state.
     */
    public StateChangeLogger(final Supplier<String> msgSupplier, final Supplier<T> supplier) {
        this.msgSupplier = msgSupplier;
        stateSupplier = supplier;
        lastState = stateSupplier.get();
    }

    /**
     * Method to determin if the state has changed.
     * @return {@code true} if the state chagned {@code false} otherwise.
     */
    private boolean checkState() {
        T state = stateSupplier.get();
        if (lastState.equals(state)) {
            return false;
        }
        lastState = state;
        return true;
    }

    /**
     * Writes a debug message if state changed
     */
    public void debug() {
        if (LOGGER.isDebugEnabled() && checkState()) {
            LOGGER.debug("{} state changed to {}", msgSupplier.get(), lastState);
        }
    }


    /**
     * Writes a n info message if state changed
     */
    public void info() {
        if (LOGGER.isInfoEnabled() && checkState()) {
            LOGGER.info("{} state changed to {}", msgSupplier.get(), lastState);
        }
    }

    /**
     * Writes a warn message if state changed
     */
    public void warn() {
        if (LOGGER.isWarnEnabled() && checkState()) {
            LOGGER.warn("{} state changed to {}", msgSupplier.get(), lastState);
        }
    }

    /**
     * Writes an error message if state changed
     */
    public void error() {
        if (LOGGER.isErrorEnabled() && checkState()) {
            LOGGER.error("{} state changed to {}", msgSupplier.get(), lastState);
        }
    }
}
