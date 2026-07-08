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
    // Reused only for the (rare) non-array-backed buffer path, to avoid a per-write allocation.
    private byte[] transferBuffer = new byte[0];
    private boolean isStreamOpen = true;

    public BlobWritableByteChannel(final BlobOutputStream blobOutputStream) {
        this.blobOutputStream = Objects.requireNonNull(blobOutputStream, "blobOutputStream cannot be null");
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        final int length = src.remaining();
        if (src.hasArray()) {
            // Common case (heap buffer, e.g. via Channels.newOutputStream): write straight from the
            // backing array. No per-call allocation and no copy - avoids heavy GC churn at high
            // throughput where the previous `new byte[remaining]` per write dominated allocations.
            final int offset = src.arrayOffset() + src.position();
            blobOutputStream.write(src.array(), offset, length);
            src.position(src.position() + length);
        } else {
            // Direct or read-only buffer: copy through a scratch buffer that is reused (and only
            // grown when needed) across writes, instead of allocating a fresh array every time.
            if (transferBuffer.length < length) {
                transferBuffer = new byte[length];
            }
            src.get(transferBuffer, 0, length);
            blobOutputStream.write(transferBuffer, 0, length);
        }
        return length;
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
