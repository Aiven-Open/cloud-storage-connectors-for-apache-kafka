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
package io.aiven.kafka.connect.common.config.extractors;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class SimpleValuePath implements DataExtractor {
    private final String[] terms;

    private SimpleValuePath(final String[] terms) {
        this.terms = terms;
    }

    /**
     * Parse a path definition string into a Path object. The path definition string is a '.' separated series of
     * strings, which are the terms in the path If the '.' is something that should be included in the terms, and you
     * want to use a different separator, then you can specify a '.' as the first character, and the separator as the
     * second character, and then the path is the rest of the string For example "a.b.c" would parse into a path with
     * terms "a", "b", "c" For example ".:a.b:c" would parse into a path with terms "a.b", "c"
     *
     * @return a PathAccess that can access a value in a nested structure
     */
    public static SimpleValuePath parse(final String pathDefinition) {
        final String pathDescription;
        final String pathSeparator;
        if (pathDefinition.length() > 1 && pathDefinition.charAt(0) == '.' ) {
            pathDescription = pathDefinition.substring(2);
            pathSeparator = pathDefinition.substring(1,2);
        } else {
            pathDescription = pathDefinition;
            pathSeparator = ".";
        }
        return new SimpleValuePath(Pattern.compile(pathSeparator, Pattern.LITERAL).split(pathDescription));
    }

    public Object extractDataFrom(final SinkRecord record) {
        Object current = record.value();

        for (final String term : terms) {
            if (current == null) {
                return null;
            }
            if (current instanceof Struct) {
                final Struct struct = (Struct) current;
                final Schema schema = struct.schema();
                final Field field = schema.field(term);
                if (field == null) {
                    return null;
                }
                current = struct.get(field);
            } else if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(term);
            } else if (current instanceof List) {
                try {
                    current = ((List<?>) current).get(Integer.parseInt(term));
                } catch (NumberFormatException|IndexOutOfBoundsException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return current;
    }

    @Override
    public String toString() {
        return "Path[terms=" + Arrays.toString( terms) +"]";
    }
}
