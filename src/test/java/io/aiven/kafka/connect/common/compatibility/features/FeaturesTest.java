/*
 * Copyright 2022 Aiven Oy
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

package io.aiven.kafka.connect.common.compatibility.features;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class FeaturesTest {

    private void testFeature(
            final String version,
            final Collection<Feature> supportedFeatures,
            final Feature feature
    ) {
        final boolean supported = supportedFeatures.contains(feature);
        final String message = String.format(
                "This feature should%s be supported with API version %s",
                supported ? "" : " not",
                version
        );
        assertEquals(supported, feature.supported(), message);
    }

    private Stream<DynamicTest> testFeatures(final String version, final Collection<Feature> supportedFeatures) {
        return DynamicTest.stream(
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT,
                        Features.SINK_TASK_ERRANT_RECORD_REPORTER,
                        Features.EXACTLY_ONCE_SOURCE_CONNECTORS
                ).iterator(),
                Object::toString,
                f -> testFeature(version, supportedFeatures, f)
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures0101() {
        return testFeatures(
                "0.10.1.0",
                Arrays.asList()
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures0102() {
        return testFeatures(
                "0102",
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT
                )
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures250() {
        return testFeatures(
                "2.5.0",
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT
                )
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures260() {
        return testFeatures(
                "2.6.0",
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT,
                        Features.SINK_TASK_ERRANT_RECORD_REPORTER
                )
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures320() {
        return testFeatures(
                "3.2.0",
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT,
                        Features.SINK_TASK_ERRANT_RECORD_REPORTER
                )
        );
    }

    @TestFactory
    public Stream<DynamicTest> testFeatures330() {
        return testFeatures(
                "3.3.0",
                Arrays.asList(
                        Features.SINK_TASK_PRE_COMMIT,
                        Features.SINK_TASK_ERRANT_RECORD_REPORTER,
                        Features.EXACTLY_ONCE_SOURCE_CONNECTORS
                )
        );
    }

}
