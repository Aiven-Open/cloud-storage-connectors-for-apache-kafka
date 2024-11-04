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

package io.aiven.kafka.connect.common.grouper;

import java.time.ZoneOffset;

import io.aiven.kafka.connect.common.config.TimestampSource;

public final class TestTimestampSource {
    private TestTimestampSource() {
    }
    @SuppressWarnings("PMD.ShortMethodName")
    public static TimestampSource of(final TimestampSource.Type type) {
        return of(type, ZoneOffset.UTC);
    }

    @SuppressWarnings("PMD.ShortMethodName")
    public static TimestampSource of(final TimestampSource.Type type, final ZoneOffset timeZone) {
        return new TimestampSource.Builder().configuration(type.toString()).zoneId(timeZone).build();
    }
}
