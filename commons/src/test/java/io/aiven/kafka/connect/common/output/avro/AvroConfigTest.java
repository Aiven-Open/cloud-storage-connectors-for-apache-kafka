/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.connect.common.output.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

final class AvroConfigTest {

    @ParameterizedTest
    @MethodSource("io.aiven.kafka.connect.common.output.avro.AvroCodecParameters#avroCodecTestParameters")
    void avroNullCodec(final String codecKey, final String codecToString) {
        final Map<String, String> properties = Map.of("avro.codec", codecKey);
        final AvroConfig avroConfig = AvroConfig.createAvroConfiguration(properties);
        assertThat(avroConfig.codecFactory()).hasToString(codecToString);
    }

    @Test
    void invalidCodec() {
        final Map<String, String> properties = Map.of("avro.codec", "invalidCodec");
        assertThatThrownBy(() -> AvroConfig.createAvroConfiguration(properties)).isInstanceOf(ConfigException.class);
    }
}
