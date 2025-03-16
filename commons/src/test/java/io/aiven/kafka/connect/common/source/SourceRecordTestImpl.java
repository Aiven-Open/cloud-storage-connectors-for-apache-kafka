package io.aiven.kafka.connect.common.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class SourceRecordTestImpl extends AbstractSourceRecord<ByteBuffer, String, OffsetManagerTest.TestingOffsetManagerEntry, SourceRecordTestImpl> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordTestImpl.class);

    public SourceRecordTestImpl(ByteBuffer nativeObject, String key) {
        super(LOGGER, new NativeInfo<ByteBuffer, String>() {
            @Override
            public ByteBuffer getNativeItem() {
                return nativeObject;
            }

            @Override
            public String getNativeKey() {
                return key;
            }

            @Override
            public long getNativeItemSize() {
                return nativeObject.capacity();
            }
        });
    }

    public SourceRecordTestImpl(SourceRecordTestImpl source) {
        super(source);;
    }

    @Override
    public SourceRecordTestImpl duplicate() {
        return new SourceRecordTestImpl(this);
    }
}
