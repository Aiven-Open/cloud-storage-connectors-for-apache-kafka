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

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestRecordGrouperBuilders {
    @SuppressWarnings("PMD.TestClassWithoutTestCases")
    public static class TestRecordGrouperBuilder implements CustomRecordGrouperBuilder {
        private Template filenameTemplate;
        private Integer maxRecordsPerFile;
        private TimestampSource timestampSource;
        private boolean schemaBased;
        private AivenCommonConfig config;

        @Override
        public void setFilenameTemplate(final Template filenameTemplate) {
            this.filenameTemplate = filenameTemplate;
        }

        @Override
        public void setMaxRecordsPerFile(final Integer maxRecordsPerFile) {
            this.maxRecordsPerFile = maxRecordsPerFile;
        }

        @Override
        public void setTimestampSource(final TimestampSource timestampSource) {
            this.timestampSource = timestampSource;
        }

        @Override
        public void setSchemaBased(final boolean schemaBased) {
            this.schemaBased = schemaBased;
        }

        @Override
        public void configure(final AivenCommonConfig config) {
            this.config = config;

        }

        @Override
        public RecordGrouper build() {
            return new TestRecordGrouper(config, filenameTemplate, maxRecordsPerFile, timestampSource, schemaBased);
        }

    }

    public static class TestRecordGrouper implements RecordGrouper {
        public final AivenCommonConfig config;
        public final Template filenameTemplate;
        public final Integer maxRecordsPerFile;
        public final TimestampSource timestampSource;
        public final boolean schemaBased;

        public TestRecordGrouper(final AivenCommonConfig config, final Template filenameTemplate,
                final Integer maxRecordsPerFile, final TimestampSource timestampSource, final boolean schemaBased) {
            this.config = config;
            this.filenameTemplate = filenameTemplate;
            this.maxRecordsPerFile = maxRecordsPerFile;
            this.timestampSource = timestampSource;
            this.schemaBased = schemaBased;
        }

        @Override
        public void put(final SinkRecord record) {

        }

        @Override
        public void clear() {

        }

        @Override
        public Map<String, List<SinkRecord>> records() {
            return Map.of();
        }
    }

}
