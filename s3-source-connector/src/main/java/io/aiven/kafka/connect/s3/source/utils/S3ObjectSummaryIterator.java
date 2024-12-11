/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implements a ObjectSummaryIterator on an S3 bucket. Implementation reads summaries in blocks and iterates over each
 * block. When block is empty a new block is retrieved and processed until no more data is available.
 */
public class S3ObjectSummaryIterator implements Iterator<S3ObjectSummary> {
    /** The client we are using */
    private final AmazonS3 s3Client;
    /** The object listing from the last call to the client */
    private ListObjectsV2Result objectListing;
    /** The inner iterator on the object summaries. When it is empty a new one is read from object listing. */
    private Iterator<S3ObjectSummary> innerIterator;

    /** the ObjectRequest initially to start the iteration from later to retrieve more records */
    private final ListObjectsV2Request request;

    /** The last key seen by this process. This allows us to restart when a new file is dropped in the direcotry */
    private String lastObjectSummaryKey;

    /**
     * Constructs the s3ObjectSummaryIterator based on the Amazon se client.
     *
     * @param s3Client
     *            the Amazon client to read use for access.
     * @param request
     *            the request object that defines the starting position for the object summary retrieval.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable AmazeonS3 and ListObjectsV2Request objects")
    public S3ObjectSummaryIterator(final AmazonS3 s3Client, final ListObjectsV2Request request) {
        this.s3Client = s3Client;
        this.request = request;
    }

    @Override
    public boolean hasNext() {
        // delay creating objectListing until we need it.
        if (objectListing == null) {
            objectListing = s3Client.listObjectsV2(request);
            innerIterator = objectListing.getObjectSummaries().iterator();
        }
        if (!this.innerIterator.hasNext()) {
            if (objectListing.isTruncated()) {
                // get the next set of data and create an iterator on it.
                request.withContinuationToken(objectListing.getContinuationToken());
                objectListing = s3Client.listObjectsV2(request);
            } else {
                // there is no more data -- reread the bucket
                request.withContinuationToken(null);
                if (lastObjectSummaryKey != null) {
                    request.withStartAfter(lastObjectSummaryKey);
                }
                objectListing = s3Client.listObjectsV2(request);
            }
            innerIterator = objectListing.getObjectSummaries().iterator();
        }
        // innerIterator is configured. Does it have more?
        return innerIterator.hasNext();
    }

    @Override
    public S3ObjectSummary next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final S3ObjectSummary result = innerIterator.next();
        lastObjectSummaryKey = result.getKey();
        return result;
    }
}
