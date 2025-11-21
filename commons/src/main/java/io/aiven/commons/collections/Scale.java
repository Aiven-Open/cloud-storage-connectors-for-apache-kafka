/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.commons.collections;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("PMD.FieldNamingConventions")
public enum Scale {
    B(1) {
        @Override
        public String format(final long byteCount) {
            return String.format("%s %s", byteCount, this.name());
        }
    },
    KB(1000), MB(KB.bytes * KB.bytes), GB(MB.bytes * KB.bytes), TB(GB.bytes * KB.bytes), PB(TB.bytes * KB.bytes),

    KiB(1024), MiB(KiB.bytes * KiB.bytes), GiB(MiB.bytes * KiB.bytes), TiB(GiB.bytes * KiB.bytes), PiB(
            TiB.bytes * KiB.bytes);

    /**
     * The IEC scale
     */
    public static final List<Scale> IEC = Collections.unmodifiableList(Arrays.asList(KiB, MiB, GiB, TiB, PiB));

    /**
     * The SI scale
     */
    @SuppressWarnings("PMD.ShortVariable")
    public static final List<Scale> SI = Collections.unmodifiableList(Arrays.asList(KB, MB, GB, TB, PB));

    final DecimalFormat dec = new DecimalFormat("0.0 ");

    public final long bytes;

    Scale(final long bytes) {
        this.bytes = bytes;
    }

    public String displayValue(final long value) {
        return format(value) + (this == B ? "" : " (" + Scale.B.format(value) + ")");
    }

    public String format(final long byteCount) {
        return dec.format(byteCount * 1.0 / bytes).concat(this.name());
    }

    public String units(final int unitCount) {
        return dec.format(unitCount).concat(this.name());
    }

    public long asBytes(final double unitCount) {
        return (long) unitCount * bytes;
    }

    public static Scale scaleOf(final long size, final List<Scale> possibleScales) {
        final List<Scale> ordered = new ArrayList<>(possibleScales);
        // sort descending size.
        ordered.sort((a, b) -> Long.compare(b.bytes, a.bytes));

        for (final Scale scale : ordered) {
            if (scale.bytes <= size) {
                return scale;
            }
        }
        return Scale.B;
    }

    public static String size(final int size, final List<Scale> possibleScales) {
        return scaleOf(size, possibleScales).format(size);
    }

}
