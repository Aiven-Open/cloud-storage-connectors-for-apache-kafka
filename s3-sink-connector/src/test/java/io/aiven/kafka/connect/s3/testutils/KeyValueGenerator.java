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

package io.aiven.kafka.connect.s3.testutils;

import java.util.ArrayList;
import java.util.List;

public class KeyValueGenerator  {

    public final IndexesToString keyGenerator;
    public final IndexesToString valueGenerator;

    public KeyValueGenerator(final IndexesToString keyGenerator,
            final IndexesToString valueGenerator) {
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
    }


    public List<KeyValueMessage> generateMessages(final int numPartitions, final int numEpochs) {
        List<KeyValueMessage> messages = new ArrayList<>();
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
