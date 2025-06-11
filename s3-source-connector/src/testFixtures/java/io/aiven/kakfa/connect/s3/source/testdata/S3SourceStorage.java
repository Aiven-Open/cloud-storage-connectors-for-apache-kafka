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

package io.aiven.kakfa.connect.s3.source.testdata;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.integration.source.SourceStorage;
import io.aiven.kafka.connect.common.storage.NativeInfo;
import io.aiven.kafka.connect.s3.source.S3SourceConnector;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.function.IOSupplier;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3SourceStorage implements SourceStorage<String, S3Object, S3OffsetManagerEntry> {
    AWSIntegrationTestData awsIntegrationTestData;
    BucketAccessor bucketAccessor;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable AWSIntegrationTestData object")
    public S3SourceStorage(final AWSIntegrationTestData awsIntegrationTestData) {
        this.awsIntegrationTestData = awsIntegrationTestData;
        this.bucketAccessor = awsIntegrationTestData.getDefaultBucketAccessor();
    }

    public void cleanup() {
        bucketAccessor.removeBucket();
        awsIntegrationTestData.tearDown();
    }

    @Override
    public String createKey(final String prefix, final String topic, final int partition) {
        return awsIntegrationTestData.createKey(prefix, topic, partition);
    }

    @Override
    public WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        return awsIntegrationTestData.writeWithKey(nativeKey, testDataBytes, bucketAccessor);
    }

    @Override
    public Map<String, String> createConnectorConfig(final String localPrefix) {
        return awsIntegrationTestData.createConnectorConfig(localPrefix, bucketAccessor.getBucketName());
    }

    @Override
    public BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }

    @Override
    public Class<? extends Connector> getConnectorClass() {
        return S3SourceConnector.class;
    }

    @Override
    public void createStorage() {

    }

    @Override
    public void removeStorage() {

    }

    @Override
    public List<? extends NativeInfo<String, S3Object>> getNativeStorage() {
        return bucketAccessor.getNativeStorage();
    }

    @Override
    public IOSupplier<InputStream> getInputStream(final String nativeKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String defaultPrefix() {
        return "";
    }
}
