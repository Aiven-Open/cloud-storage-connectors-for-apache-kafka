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

package io.aiven.kafka.connect.azure.source;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.azure.source.utils.Version;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobSourceTask extends AbstractSourceTask {
    /* The logger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceTask.class);
    /**
     * Constructor to set the Logger used.
     *
     */
    protected AzureBlobSourceTask() {
        super(LOGGER);
    }

    @Override
    protected Iterator<SourceRecord> getIterator(final BackoffConfig config) {
        return null;
    }

    @Override
    protected SourceCommonConfig configure(final Map<String, String> props) {
        return null;
    }

    @Override
    protected void closeResources() {

    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
