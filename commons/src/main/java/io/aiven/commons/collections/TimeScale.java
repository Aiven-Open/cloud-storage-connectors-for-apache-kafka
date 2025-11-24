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

/**
 * Scales milliseconds into standard human time units and standard human time units into milliseconds. Also provides
 * formatting for the same.
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public enum TimeScale {
    MILLISECONDS(1) {
        @Override
        public String format(final long milliseconds) {
            return String.format("%s %s", milliseconds, unitName());
        }
    },
    SECONDS(MILLISECONDS.milliseconds * 1000), MINUTES(SECONDS.milliseconds * 60), HOURS(
            MINUTES.milliseconds * 60), DAYS(HOURS.milliseconds * 24);

    /**
     * The Decimal format for the TimeUnit displays.
     */
    final DecimalFormat dec = new DecimalFormat("0.0 ");

    /**
     * The number of milliseconds in one TimeUnit.
     */
    public final long milliseconds;

    /**
     * Definition of a time unit in the number of milliseconds.
     *
     * @param milliseconds
     *            the number of milliseconds in the time unit.
     */
    TimeScale(final long milliseconds) {
        this.milliseconds = milliseconds;
    }

    /**
     * Format the number of milliseconds in this TimeScale along with the number of Milliseconds. For example "3.5
     * minutes (210000 milliseconds)". If the Timescale is MILLISECONDS then the parenthetical portion is not displayed.
     *
     * @param milliseconds
     *            the number of milliseconds to format.
     * @return the String representation of the number and TimeUnit as well as the number of milliseconds.
     */
    public String displayValue(final long milliseconds) {
        return format(milliseconds) + (this == MILLISECONDS ? "" : " (" + MILLISECONDS.format(milliseconds) + ")");
    }

    /**
     * Gets the name of the units for this TimeScale.
     *
     * @return the name of the units.
     */
    public String unitName() {
        return this.name().charAt(0) + this.name().substring(1).toLowerCase(Locale.ROOT);
    }

    /**
     * Format the number of milliseconds into this TimeScale.
     *
     * @param milliseconds
     *            the number of milliseconds.
     * @return a formatted string representation.
     */
    public String format(final long milliseconds) {
        return dec.format(milliseconds * 1.0 / this.milliseconds).concat(unitName());
    }

    /**
     * Format the number of units of this TimeScale for output.
     *
     * @param unitCount
     *            the number of TimeScale units.
     * @return the formatted string.
     */
    public String units(final int unitCount) {
        return dec.format(unitCount * milliseconds).concat(unitName());
    }

    /**
     * Gets the number of milliseconds in the {@code unitcount} number of TimeUnits.
     *
     * @param unitCount
     *            the number of TimeUnits
     * @return the number of milliseconds.
     */
    public long asMilliseconds(final double unitCount) {
        return (long) unitCount * milliseconds;
    }

    /**
     * Returns the largest TimeScale that will represent {@code milliseconds} with at least 1 unit.
     *
     * @param milliseconds
     *            the number of milliseconds
     * @return the TimeScale for the milliseconds.
     */
    public static TimeScale scaleOf(final long milliseconds) {
        final List<TimeScale> ordered = new ArrayList<>(Arrays.asList(TimeScale.values()));
        // sort descending size.
        ordered.sort((a, b) -> Long.compare(b.milliseconds, a.milliseconds));

        for (final TimeScale scale : ordered) {
            if (scale.milliseconds <= milliseconds) {
                return scale;
            }
        }
        return MILLISECONDS;
    }

    /**
     * Formats the milliseconds into a TimeScale that best fits the number of milliseconds.
     *
     * @param milliseconds
     *            the number of milliseconds.
     * @return the String representation.
     * @see #scaleOf(long)
     */
    public static String size(final int milliseconds) {
        return scaleOf(milliseconds).format(milliseconds);
    }

}
