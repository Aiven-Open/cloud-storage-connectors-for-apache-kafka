package io.aiven.kafka.connect.s3.source.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements a ObjectSummaryIterator on an S3 bucket.
 * Implementation reads summaries in blocks and iterates over each block.  When block is empty
 * a new block is retrieved and processed until no more data is available.
 */
public class S3ObjectSummaryIterator implements Iterator<S3ObjectSummary> {
    /** The client we are using */
    private final AmazonS3 s3Client;
    /** The object listing from the last call to the client */
    private ListObjectsV2Result objectListing;
    /** The inner iterator on the object summaries.  When it is empty a new one is read from object listing.*/
    private Iterator<S3ObjectSummary> innerIterator;

    /** the ObjectRequest to start the iteration from */
    private ListObjectsV2Request req;

    /**
     * Constructs the s3ObjectSummaryIterator based on the Amazon se client.
     * @param s3Client the Amazon client to read use for access.
     * @param bucketName the name of the bucket to read.
     * @param maxKeys the maximum number of keys to read
     */
    public S3ObjectSummaryIterator(final AmazonS3 s3Client, final String bucketName, final int maxKeys, String startAfter) {
        this.s3Client = s3Client;
        req = new ListObjectsV2Request().withBucketName(bucketName)
                .withMaxKeys(maxKeys).withStartAfter(startAfter);
    }

    @Override
    public boolean hasNext() {
        // delay creating objectListing until we need it.
        if (objectListing == null) {
            this.objectListing = s3Client.listObjectsV2(req);
            this.innerIterator = objectListing.getObjectSummaries().iterator();
        }
        if (!this.innerIterator.hasNext()) {
            if (!objectListing.isTruncated()) {
                // there is no more data
                return false;
            }
            // get the next set of data and create an iterator on it.
            objectListing = s3Client.listObjectsV2(new ListObjectsV2Request()
                    .withBucketName(objectListing.getBucketName())
                    .withContinuationToken(objectListing.getContinuationToken())
                    .withMaxKeys(objectListing.getMaxKeys()));
            innerIterator = objectListing.getObjectSummaries().iterator();
        }
        // innerIterator is configured.  Does it have more?
        return innerIterator.hasNext();
    }

    @Override
    public S3ObjectSummary next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return innerIterator.next();
    }
}