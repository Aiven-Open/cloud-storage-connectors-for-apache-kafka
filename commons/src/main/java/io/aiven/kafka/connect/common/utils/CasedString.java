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

package io.aiven.kafka.connect.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.text.WordUtils;

/**
 * Handles converting from one string case to another (e.g. camel case to snake case).
 */
public class CasedString {
    /** the string of the cased format. */
    private final String string;
    /** the case of the string. */
    private final StringCase stringCase;

    /**
     * A method to join camel string fragments together.
     */
    private static final Function<String[], String> CAMEL_JOINER = a -> {
        final StringBuilder builder = new StringBuilder(a[0].toLowerCase(Locale.ROOT));

        for (int i = 1; i < a.length; i++) {
            builder.append(WordUtils.capitalize(a[i].toLowerCase(Locale.ROOT)));
        }
        return builder.toString();
    };

    /**
     * An enumeration of supported string cases. These cases tag strings as having a specific format.
     */
    public enum StringCase {
        /**
         * Camel case tags strings like 'CamelCase' or 'camelCase'. This conversion forces the first character to lower
         * case. If specific capitalization rules are required use {@link WordUtils#capitalize(String)} to set the first
         * character of the string.
         */
        CAMEL(Character::isUpperCase, true, CAMEL_JOINER),
        /**
         * Snake case tags strings like 'Snake_Case'. This conversion does not change the capitalization of any
         * characters in the string. If specific capitalization is required use {@link String#toUpperCase()},
         * {@link String#toLowerCase()}, or the commons-text methods {@link WordUtils#capitalize(String)}, or
         * {@link WordUtils#uncapitalize(String)} as required.
         */
        SNAKE(c -> c == '_', false, a -> String.join("_", a)),
        /**
         * Kebab case tags strings like 'kebab-case'. This conversion does not change the capitalization of any
         * characters in the string. If specific capitalization is required use {@link String#toUpperCase()},
         * {@link String#toLowerCase()}, * or the commons-text methods {@link WordUtils#capitalize(String)}, or
         * {@link WordUtils#uncapitalize(String)} as required.
         */
        KEBAB(c -> c == '-', false, a -> String.join("-", a)),

        /**
         * Phrase case tags phrases of words like 'phrase case'. This conversion does not change the capitalization of
         * any characters in the string. If specific capitalization is required use {@link String#toUpperCase()},
         * {@link String#toLowerCase()}, * or the commons-text methods {@link WordUtils#capitalize(String)}, or
         * {@link WordUtils#uncapitalize(String)} as required.
         */
        PHRASE(Character::isWhitespace, false, a -> String.join(" ", a)),

        /**
         * Dot case tags phrases of words like 'phrase.case'. This conversion does not change the capitalization of any
         * characters in the string. If specific capitalization is required use {@link String#toUpperCase()},
         * {@link String#toLowerCase()}, * or the commons-text methods {@link WordUtils#capitalize(String)}, or
         * {@link WordUtils#uncapitalize(String)} as required.
         */
        DOT(c -> c == '.', false, a -> String.join(".", a));

        /** The segment value for a null string */
        private static final String[] NULL_SEGMENT = new String[0];
        /** The segment value for an empty string */
        private static final String[] EMPTY_SEGMENT = { "" };

        /** test for split position character. */
        private final Predicate<Character> splitter;
        /** if {@code true} split position character will be preserved in following segment. */
        private final boolean preserveSplit;
        /** a function to joining the segments into this case type. */
        private final Function<String[], String> joiner;

        /**
         * Defines a String Case.
         *
         * @param splitter
         *            The predicate that determines when a new word in the cased string begins.
         * @param preserveSplit
         *            if {@code true} the character that the splitter detected is preserved as the first character of
         *            the new word.
         * @param joiner
         *            The function to merge a list of strings into the cased String.
         */
        StringCase(final Predicate<Character> splitter, final boolean preserveSplit,
                final Function<String[], String> joiner) {
            this.splitter = splitter;
            this.preserveSplit = preserveSplit;
            this.joiner = joiner;
        }

        /**
         * Creates a cased string from a collection of segments.
         *
         * @param segments
         *            the segments to create the CasedString from.
         * @return a CasedString
         */
        public String assemble(final String[] segments) {
            return segments.length == 0 ? null : this.joiner.apply(segments);
        }

        /**
         * Returns an array of each of the segments in this CasedString. Segments are defined as the strings between the
         * separators in the CasedString. For the CAMEL case the segments are determined by the presence of a capital
         * letter.
         *
         * @return the array of Strings that are segments of the cased string.
         */
        public String[] getSegments(final String string) {
            if (string == null) {
                return NULL_SEGMENT;
            }
            if (string.isEmpty()) {
                return Arrays.copyOf(EMPTY_SEGMENT, 1);
            }
            final List<String> lst = new ArrayList<>();
            final StringBuilder builder = new StringBuilder();
            for (final char c : string.toCharArray()) {
                if (splitter.test(c)) {
                    if (builder.length() > 0) {
                        lst.add(builder.toString());
                        builder.setLength(0);
                    }
                    if (preserveSplit) {
                        builder.append(c);
                    }
                } else {
                    builder.append(c);
                }
            }
            if (builder.length() > 0) {
                lst.add(builder.toString());
            }
            return lst.toArray(new String[0]);
        }
    }

    /**
     * A representation of a cased string and the identified case of that string.
     *
     * @param stringCase
     *            The {@code StringCase} that the {@code string} argument is in.
     * @param string
     *            The string.
     */
    public CasedString(final StringCase stringCase, final String string) {
        this.string = string == null ? null : stringCase.assemble(stringCase.getSegments(string.trim()));
        this.stringCase = stringCase;
    }

    /**
     * Returns an array of each of the segments in this CasedString. Segments are defined as the strings between the
     * separators in the CasedString. For the CAMEL case the segments are determined by the presence of a capital
     * letter.
     *
     * @return the array of Strings that are segments of the cased string.
     */
    public String[] getSegments() {
        return stringCase.getSegments(string);
    }

    /**
     * Converts this cased string into a {@code String} of another format. The upper/lower case of the characters within
     * the string are not modified.
     *
     * @param stringCase
     *            The format to convert to.
     * @return the String current string represented in the new format.
     */
    public String toCase(final StringCase stringCase) {
        if (stringCase == this.stringCase) {
            return string;
        }
        return string == null ? null : stringCase.joiner.apply(getSegments());
    }

    /**
     * Returns the string representation provided in the constructor.
     *
     * @return the string representation.
     */
    @Override
    public String toString() {
        return string;
    }
}
