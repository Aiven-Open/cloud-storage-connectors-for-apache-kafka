/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.common;

import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Version {

    private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);

    private Version() {

    }

    public static String getVersion(final String propertyFilename) {
        final Properties props = new Properties();
        try (final InputStream in = Version.class.getClassLoader().getResourceAsStream(propertyFilename)) {
            if (Objects.isNull(in)) {
                LOGGER.warn("Couldn't find property file: " + propertyFilename);
            } else {
                props.load(in);
            }
        } catch (final Exception e) {
            LOGGER.warn("Error while loading {}: {}", propertyFilename, e.getMessage());
        }
        return props.getProperty("version", "unknown").trim();
    }

}
