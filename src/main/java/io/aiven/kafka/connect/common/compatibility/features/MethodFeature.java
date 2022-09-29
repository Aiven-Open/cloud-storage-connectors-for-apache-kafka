/*
 * Copyright 2022 Aiven Oy
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

package io.aiven.kafka.connect.common.compatibility.features;

import java.util.List;

/**
 * A {@link Feature} whose availability can be determined by checking for the existence of a method at runtime via
 * reflection.
 */
abstract class MethodFeature extends Feature {

    protected boolean checkSupported() {
        try {
            final Class<?> klass = klass();
            klass.getMethod(method(), parameters().toArray(new Class<?>[0]));
            return true;
        } catch (final NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * @return the class whose method should be checked to determine if the feature is supported
     */
    protected abstract Class<?> klass();

    /**
     * @return the name of the method that should be checked to determine if the feature is supported
     */
    protected abstract String method();

    /**
     * @return the parameter types of the method that should be checked to determine if the feature is supported
     */
    protected abstract List<Class<?>> parameters();

}
