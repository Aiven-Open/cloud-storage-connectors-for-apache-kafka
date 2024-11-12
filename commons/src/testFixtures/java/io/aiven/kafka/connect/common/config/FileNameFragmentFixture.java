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

package io.aiven.kafka.connect.common.config;

public class FileNameFragmentFixture {// NOPMD

    public enum FileNameArgs {
        GROUP_FILE(FileNameFragment.GROUP_FILE), FILE_COMPRESSION_TYPE_CONFIG(
                FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG), FILE_MAX_RECORDS(
                        FileNameFragment.FILE_MAX_RECORDS), FILE_NAME_TIMESTAMP_TIMEZONE(
                                FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE), FILE_NAME_TIMESTAMP_SOURCE(
                                        FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE), FILE_NAME_TEMPLATE_CONFIG(
                                                FileNameFragment.FILE_NAME_TEMPLATE_CONFIG), DEFAULT_FILENAME_TEMPLATE(
                                                        FileNameFragment.DEFAULT_FILENAME_TEMPLATE);
        final String key;// NOPMD
        FileNameArgs(final String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    public FileNameFragmentFixture() {
        // do
    }
    public String get() {
        return "stuff";
    }
}
