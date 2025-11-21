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
import java.util.List;
import java.util.Locale;

@SuppressWarnings("PMD.FieldNamingConventions")
public enum TimeScale {
    MILLISECONDS(1) {
        @Override
        public String format(final long milliseconds ) {
            return String.format("%s %s", milliseconds, this.name());
        }
    },
    SECONDS(MILLISECONDS.milliseconds * 1000),
    MINUTES( SECONDS.milliseconds * 60),
    HOURS(MINUTES.milliseconds * 60 ),
    DAYS(HOURS.milliseconds * 24);


    final DecimalFormat dec = new DecimalFormat("0.0 ");

    public final long milliseconds;

    TimeScale(final long milliseconds) {
        this.milliseconds  = milliseconds;
    }

    public String displayValue(final long value) {
        return format(value) + (this == MILLISECONDS ? "" : " (" + MILLISECONDS.format(value) + ")");
    }

    private String unitName() {
        return this.name().charAt(0) + this.name().substring(1).toLowerCase(Locale.ROOT);
    }

    public String format(final long milliseconds) {
        double unitCount = milliseconds * 1.0 / this.milliseconds;
        return dec.format(unitCount).concat(unitName());
    }

    public String units(final int unitCount) {
        return dec.format(unitCount * milliseconds).concat(this.name());
    }

    public long asMilliseconds(final double unitCount) {
        return (long) unitCount * milliseconds;
    }

    public static TimeScale scaleOf(final long size) {
        final List<TimeScale> ordered = new ArrayList<>(Arrays.asList(TimeScale.values()));
        // sort descending size.
        ordered.sort((a, b) -> Long.compare(b.milliseconds, a.milliseconds));

        for (final TimeScale scale : ordered) {
            if (scale.milliseconds <= size) {
                return scale;
            }
        }
        return MILLISECONDS;
    }

    public static String size(final int size) {
        return scaleOf(size).format(size);
    }

}
