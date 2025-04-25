package io.aiven.kafka.connect.common.integration;


import io.aiven.kafka.connect.common.source.NativeInfo;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.connector.Connector;

import java.io.InputStream;
import java.util.List;

public interface StorageBase<N, K extends Comparable<K>> {
   Class<? extends Connector> getConnectorClass();
    void createStorage();
    void removeStorage();
    /**
     * Retrieves a list of {@link NativeInfo} implementations, one for each item in native storage.
     *
     * @return the list of {@link NativeInfo} implementations, one for each item in native storage.
     */
    List<? extends NativeInfo<N, K>> getNativeStorage();
    IOSupplier<InputStream> getInputStream(K nativeKey);
    String defaultPrefix();
}
