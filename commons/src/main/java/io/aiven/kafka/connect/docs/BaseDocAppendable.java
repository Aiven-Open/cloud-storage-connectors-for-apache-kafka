/*
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package io.aiven.kafka.connect.docs;

import java.io.IOException;

/**
 * An abstract implementation of {@link DocAppendable} that writes output to an {@link Appendable} instance.
 * <p>
 * This class is the superclass of all classes that filter help appendables. These appendable sit on top of an existing appendable (the <em>underlying</em>
 * appendable) which it uses as its basic sink of data, but possibly transforming the data along the way or providing additional functionality.
 * </p>
 * <p>
 * The class {@code BaseDocAppendable} itself simply overrides all methods of {@code DocAppendable} with versions that pass all requests to the underlying
 * appendable. Subclasses of {@code BaseDocAppendable} may further override some of these methods as well as provide additional methods and fields.
 * </p>
 * <p>
 * <em>Implementation Note</em>: This class is similar to FilterOutputStream in relation to OutputStream. We could further split BaseDocAppendable into a
 * FilterAppendable but that seems like YAGNI.
 * </p>
 *
 * @since 1.10.0
 */
public abstract class BaseDocAppendable implements DocAppendable {

    /**
     * The underlying appendable to be filtered.
     */
    protected final Appendable output;

    /**
     * Constructs an appendable filter built on top of the specified underlying appendable.
     *
     * @param output the underlying appendable to be assigned to the field {@code this.output} for later use, or {@code null} if this instance is to be created
     *               without an underlying stream.
     */
    protected BaseDocAppendable(final Appendable output) {
        this.output = output;
    }

    @Override
    public BaseDocAppendable append(final char ch) throws IOException {
        output.append(ch);
        return this;
    }

    @Override
    public BaseDocAppendable append(final CharSequence text) throws IOException {
        output.append(text);
        return this;
    }

    @Override
    public BaseDocAppendable append(final CharSequence csq, final int start, final int end) throws IOException {
        output.append(csq, start, end);
        return this;
    }
}
