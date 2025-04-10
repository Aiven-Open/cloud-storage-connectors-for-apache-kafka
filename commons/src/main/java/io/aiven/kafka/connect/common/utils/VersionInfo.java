
/*
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 */

package io.aiven.kafka.connect.common.utils;
/**
 * This class was originally developed by the Apache RAT project
 * A formatter for Package information about a class.
 * @see Package
 */
public final class VersionInfo {
    /**
     * The version info string.
     */
    private final Package pkg;
    
    private String orDefault(final String value, final String defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * Constructor that uses the VersionInfo package for information.
     */
    public VersionInfo() {
        this(VersionInfo.class);
    }

    /**
     * Constructor for a specific class.
     * @param clazz the class to get the Package information from.
     */
    public VersionInfo(final Class<?> clazz) {
        pkg = clazz.getPackage();
    }

    /**
     * Default string representation of the implementation information from the package.
     * @return The string representation.
     */
    @Override
    public String toString() {
        return String.format("%s %s (%s)", getTitle(), getVersion(), getVendor());
    }

    /**
     * Gets the implementation version of the package. Will return "VERSION-NUMBER" if
     * package information is not available.
     * @return the implementation version.
     */
    public String getVersion() {
        return orDefault(pkg.getImplementationVersion(), "VERSION-NUMBER");
    }

    /**
     * Gets the implementation vendor of the package. Will return "VENDOR-NAME" if
     * package information is not available.
     * @return the implementation vendor
     */
    public String getVendor() {
        return orDefault(pkg.getImplementationVendor(), "VENDOR-NAME");
    }

    /**
     * Gets the implementation title of the package. Will return "TITLE" if
     * package information is not available.
     * @return the implementation title
     */
    public String getTitle() {
        return orDefault(pkg.getImplementationTitle(), "TITLE");
    }

    /**
     * Gets the specification version of the package. Will return "SPEC-VERSION" if
     * package information is not available.
     * @return the specification version.
     */
    public String getSpecVersion() {
        return orDefault(pkg.getSpecificationVersion(), "SPEC-VERSION");
    }

    /**
     * Gets the specification vendor of the package. Will return "SPEC-VENDOR" if
     * package information is not available.
     * @return the specification vendor
     */
    public String getSpecVendor() {
        return orDefault(pkg.getSpecificationVendor(), "SPEC-VENDOR");
    }

    /**
     * Gets the specification title of the package. Will return "SPEC-TITLE" if
     * package information is not available.
     * @return the specification title
     */
    public String getSpecTitle() {
        return orDefault(pkg.getSpecificationTitle(), "SPEC-TITLE");
    }
}