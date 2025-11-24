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

public class BackoffPolicyFragmentFixture {// NOPMD

    public enum BackoffPolicyArgs {
        GROUP_RETRY_BACKOFF_POLICY(BackoffPolicyFragment.GROUP_RETRY_BACKOFF_POLICY), KAFKA_RETRY_BACKOFF_MS_CONFIG(
                BackoffPolicyFragment.KAFKA_RETRY_BACKOFF_MS_CONFIG);
        final String key;// NOPMD
        BackoffPolicyArgs(final String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    public BackoffPolicyFragmentFixture() {
        // do
    }
    public String get() {
        return "stuff";
    }
}
