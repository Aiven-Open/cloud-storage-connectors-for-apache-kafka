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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.utils.VersionInfo;
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
    private AzureBlobSourceConfig azureBlobSourceConfig; // NOPMD only called once, when used in the future this can be
                                                         // removed

    @Override
    protected Iterator<SourceRecord> getIterator(final BackoffConfig config) {
        return Collections.emptyIterator();
    }

    @Override
    protected SourceCommonConfig configure(final Map<String, String> props) {
        this.azureBlobSourceConfig = new AzureBlobSourceConfig(props);
        return azureBlobSourceConfig;
    }

    @Override
    protected void closeResources() {

    }

    @Override
    public String version() {
        return new VersionInfo().getVersion();
    }
}
