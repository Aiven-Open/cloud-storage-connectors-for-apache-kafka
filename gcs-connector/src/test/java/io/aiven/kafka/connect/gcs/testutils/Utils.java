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

package io.aiven.kafka.connect.gcs.testutils;

final public class Utils {
    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    private Utils() {
        /* hide constructor */ }

    public static String bytesToHex(final byte[] bytes) {
        final char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            final int value = bytes[i] & 0xFF;
            chars[i * 2] = HEX_CHARS[value >>> 4];
            chars[i * 2 + 1] = HEX_CHARS[value & 0x0F];
        }
        return new String(chars);
    }
}
