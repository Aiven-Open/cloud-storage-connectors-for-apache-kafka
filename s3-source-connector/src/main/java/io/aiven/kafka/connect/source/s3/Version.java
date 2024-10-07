/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.source.s3;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Version {
    private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);

    private static final String PROPERTIES_FILENAME = "s3-source-connector-for-apache-kafka-version.properties";

    static final String VERSION; // NOPMD AvoidFieldNameMatchingTypeName

    static {
        final Properties props = new Properties();
        try (InputStream resourceStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(PROPERTIES_FILENAME)) {
            props.load(resourceStream);
        } catch (final Exception e) { // NOPMD AvoidCatchingGenericException
            LOGGER.warn("Error while loading {}: {}", PROPERTIES_FILENAME, e.getMessage());
        }
        VERSION = props.getProperty("version", "unknown").trim();
    }
}
