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

package io.aiven.kafka.connect.common.source.api;

import java.util.Iterator;

/**
 * The SourceApiClient allows us to implement a common interface for all sources to list and retrieve objects which can
 * be then processed and added to a kafka topic.
 *
 * @param <T>
 *            The Returned Record type expected from the API.
 */
public interface SourceApiClient<T> {

    /**
     * @param startToken
     *            the startToken can optionally be passed to the API to tell it where to start listing objects from. Not
     *            all APIs might support this parameter and it can be null but it can be used to speed up the time it
     *            takes to start processing new records on startup.
     * @return an Iterator of object keys that are available to be consumed by the source connector the object key
     *         references an exact object stored in the source system
     */
    Iterator<String> getListOfObjects(String startToken);

    /**
     *
     * @param ObjectKey
     *            is the unique identifier that will identify the exact object to return from the api.
     * @return a specific Object from the API
     */
    T getObject(String ObjectKey);

    /**
     * Allows the implmenetation to build a map of entries that could not be processed.
     *
     * @param objectKey
     *            a unique key identifying an object that was not processable and was skipped.
     */
    void addFailedObjectKeys(String objectKey);
}
