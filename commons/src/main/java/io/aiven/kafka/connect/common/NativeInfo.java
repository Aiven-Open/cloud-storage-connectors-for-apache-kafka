package io.aiven.kafka.connect.common;

/**
 * Information about the Native object.
 *
 * @param <N> The native object type.
 * @param <K> the native key type
 */
public interface NativeInfo<K, N> {
    /**
     * Gets the native item.
     *
     * @return The native item.
     */
    N getNativeItem();

    /**
     * Gets the native key
     *
     * @return The Native key.
     */
    K getNativeKey();

    /**
     * Gets the number of bytes in the input stream extracted from the native object.
     *
     * @return The number of bytes in the input stream extracted from the native object.
     */
    long getNativeItemSize();
}
