/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.EscapeTool;

public class ConfigDoc {

    @Override
    public String toString() {
        return "Documentation configuration";
    }

    public static void execute(final ConfigDef configDef, final String templateFile, final String output)
            throws IOException {
        final VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();

        final Template template = velocityEngine.getTemplate(templateFile);
        final VelocityContext context = new VelocityContext();

        final Collection<ConfigDef.ConfigKey> keys = configDef.configKeys().values();
        final Map<String, ConfigData> sections = new TreeMap<>();
        for (final ConfigDef.ConfigKey key : keys) {
            sections.put(key.name, new ConfigData(key)); // NOPMD AvoidInstantiatingObjectsInLoops
        }

        context.put("sections", sections.values());
        context.put("esc", new EscapeTool());

        try (final FileWriter writer = new FileWriter(output, StandardCharsets.UTF_8)) { // NOPMD
            // AvoidFileStream
            template.merge(context, writer);
        }
    }

    public static void main(final String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        if (args.length == 3) { // NOPMD AvoidLiteralsInIfCondition
            final ConfigDef configDef = (ConfigDef) Class.forName(args[0]).getConstructor().newInstance();
            execute(configDef, args[1], args[2]);
        }
        if (args.length == 4) { // NOPMD AvoidLiteralsInIfCondition
            final Method method = Class.forName(args[0]).getDeclaredMethod(args[1]);
            final ConfigDef configDef = (ConfigDef) method.invoke(null);
            execute(configDef, args[2], args[3]);
        }
    }

}
