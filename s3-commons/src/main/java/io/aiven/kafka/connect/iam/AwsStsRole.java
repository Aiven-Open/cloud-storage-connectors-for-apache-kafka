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

package io.aiven.kafka.connect.iam;

import java.util.Objects;


public final class AwsStsRole {

    private final String arn;
    private final String externalId;
    private final String sessionName;
    private final int sessionDurationSeconds;

    public AwsStsRole(final String arn, final String externalId, final String sessionName,
            final int sessionDurationSeconds) {
        this.arn = arn;
        this.externalId = externalId;
        this.sessionName = sessionName;
        this.sessionDurationSeconds = sessionDurationSeconds;
    }

    public String getArn() {
        return arn;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getSessionName() {
        return sessionName;
    }

    public int getSessionDurationSeconds() {
        return sessionDurationSeconds;
    }

    public Boolean isValid() {
        return Objects.nonNull(arn) && Objects.nonNull(sessionName);
    }
}
