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

package io.aiven.kafka.connect.common.integration.sink;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates key and values for testing.
 */
public class KeyValueGenerator {
    /** the function to generate the key string */
    public final IndexesToString keyGenerator;
    /** the function to generate the value string */
    public final IndexesToString valueGenerator;

    /**
     * Creates a KeyValueGenerator with the specified key and value generators.
     *
     * @param keyGenerator
     *            the function to generate the key string
     * @param valueGenerator
     *            the function to generate the value string
     */
    public KeyValueGenerator(final IndexesToString keyGenerator, final IndexesToString valueGenerator) {
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
    }

    /**
     * Generates a list of KeyValueMessages.
     *
     * @param numPartitions
     *            the number of partitions to generate values for.
     * @param numEpochs
     *            the number of epochs to generate values for.
     * @return a list of KeyValueMessages, one for each partition/epoch pair.
     */
    public List<KeyValueMessage> generateMessages(final int numPartitions, final int numEpochs) {
        final List<KeyValueMessage> messages = new ArrayList<>();
        int idx = 0;
        for (int epoch = 0; epoch < numEpochs; epoch++) {
            for (int partition = 0; partition < numPartitions; partition++) {
                messages.add(new KeyValueMessage(keyGenerator.generate(partition, epoch, idx),
                        valueGenerator.generate(partition, epoch, idx), partition, idx++, epoch));
            }
        }
        return messages;
    }
}
