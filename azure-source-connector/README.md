# Aiven's Azure Blob Storage Source Connector for Apache Kafka®

The Azure source connector follows the general source processes outline in the Source Design Strategy document.  It does not deviate from that process.

Objects in the data store are retrieved in the lexical order that the Azure blob storage stores them.


## Config
The property `native.start.key` can be used with a continuationToken, if the data in the container is static and no blobs are being added or deleted.
Discussion on continuationToken on github [here]('https://github.com/Azure/azure-sdk-for-net/issues/17222#issuecomment-736721165')
