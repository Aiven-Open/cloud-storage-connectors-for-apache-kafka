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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;
import org.apache.velocity.tools.generic.EscapeTool;

/**
 * Uses Apache Velocity to write a configuration document.
 *
 * @see <a href="https://velocity.apache.org/">Apache Velocity</a>
 */
public class ConfigDoc {

    @Override
    public String toString() {
        return "Documentation configuration";
    }

    /**
     * Writes the ConfigDef documentation to the output based no the tempplate.
     *
     * @param configDef
     *            the ConfigDef to generate the documentation for.
     * @param templateFile
     *            name of the template file to use.
     * @param output
     *            the name of the output file.
     * @throws IOException
     *             on IO error.
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public static void execute(final ConfigDef configDef, final String templateFile, final String output)
            throws IOException {
        final VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "file");
        velocityEngine.setProperty("file.resource.loader.class", FileResourceLoader.class.getName());

        velocityEngine.init();

        final VelocityContext context = new VelocityContext();

        final Collection<ConfigDef.ConfigKey> keys = configDef.configKeys().values();
        final Map<String, ConfigData> sections = new TreeMap<>();
        for (final ConfigDef.ConfigKey key : keys) {
            if (!key.internalConfig) {
                sections.put(key.name, new ConfigData(key));
            }
        }

        context.put("sections", sections.values());
        context.put("esc", new Escaper());

        final File file = new File(output);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new IOException("Unable to create directory: " + file.getParentFile());
        }
        final Template template = velocityEngine.getTemplate(templateFile);
        try (BufferedWriter writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
            template.merge(context, writer);
        }
    }

    /**
     * Executes the generation of documentation from a configuration definition. THis method takes three (3) or four (4)
     * arguments.
     *
     * Three argument version
     * <ol>
     * <li>Class name - Name of the class to generate the documentation from. Must implement {@link ConfigDef}</li>
     * <li>Template - The file name of the Velocity template</li>
     * <li>Output - THe file name for the generated document</li>
     * </ol>
     *
     * Four argument version
     * <ol>
     * <li>Class name - Name of the class to generate the documentation from.</li>
     * <li>Static method name - The name of a static method on the class that produces a {@link ConfigDef} object.</li>
     * <li>Template - The file name of the Velocity template</li>
     * <li>Output - THe file name for the generated document</li>
     * </ol>
     *
     * @param args
     *            the arguments
     * @throws IOException
     *             on IO error.
     * @throws ClassNotFoundException
     *             if the configuration is not found.
     * @throws NoSuchMethodException
     *             if the method name is not found.
     * @throws InvocationTargetException
     *             if the method can not be invoked.
     * @throws InstantiationException
     *             if the class can not be instantiated.
     * @throws IllegalAccessException
     *             if there are access restrictions on the class.
     */
    @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
    public static void main(final String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        if (args.length == 3) {
            final ConfigDef configDef = (ConfigDef) Class.forName(args[0]).getConstructor().newInstance();
            execute(configDef, args[1], args[2]);
        }
        if (args.length == 4) {
            final Method method = Class.forName(args[0]).getDeclaredMethod(args[1]);
            final ConfigDef configDef = (ConfigDef) method.invoke(null);
            execute(configDef, args[2], args[3]);
        }
    }

    public static class Escaper extends EscapeTool {

        private static String[] charParser(final String charText) {
            char[] chars = charText.toCharArray();
            String[] result = new String[chars.length];
            for (int i = 0; i < chars.length; i++) {
                result[i] = String.valueOf(chars[i]);
            }
            return result;
        }

        /**
         * The characters to escape for markdown.
         */
        private static final String[] MARKDOWN_CHARS = charParser("\\`*_{}[]<>()#+-.!|");
        /**
         * The characters to escape for APT (Almost Plain Text).
         */
        private static final String[] APT_CHARS = charParser("\\~=-+*[]<>{}");

        /**
         * Escapes a text string.
         *
         * @param text
         *            the text to escape.
         * @param chars
         *            the characters to escape.
         * @return the escaped string.
         */
        private String escape(final String text, final String[] chars) {
            if (text == null) {
                return "";
            }
            String result = text;
            for (String c : chars) {
                result = result.replace(c, "\\" + c);
            }
            return result;
        }

        /**
         * Escapes a string for markdown.
         *
         * @param text
         *            the text to escape.
         * @return the text with the markdown specific characters escaped.
         */
        public String markdown(final String text) {
            return escape(text, MARKDOWN_CHARS);
        }

        /**
         * Escapes a string for APT (almost plain text).
         *
         * @param text
         *            the text to escape.
         * @return the text with the APT specific characters escaped.
         */
        public String apt(final String text) {
            return escape(text, APT_CHARS);
        }

    }
}
