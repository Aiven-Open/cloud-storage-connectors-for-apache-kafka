/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.azure.sink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

import com.azure.storage.blob.specialized.BlobOutputStream;

public class BlobWritableByteChannel implements WritableByteChannel {
    private final BlobOutputStream blobOutputStream;
    private boolean isStreamOpen = true;

    public BlobWritableByteChannel(final BlobOutputStream blobOutputStream) {
        this.blobOutputStream = Objects.requireNonNull(blobOutputStream, "blobOutputStream cannot be null");
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        final int bytesWritten = src.remaining();
        final byte[] buffer = new byte[bytesWritten];
        src.get(buffer);
        blobOutputStream.write(buffer);
        return bytesWritten;
    }

    @Override
    public boolean isOpen() {
        return isStreamOpen;
    }

    @Override
    public void close() throws IOException {
        if (isStreamOpen) {
            blobOutputStream.close();
            isStreamOpen = false;
        }
    }
}
