# Source design strategy

This document describes the design for source connectors and how to think about implementing one.

## Overview

The `AbstractSourceTask` handles most of the issues around synchronization and communicating with the upstream connector framework.  Implementors of concrete SourceTasks need only concern themselves with reading data from the data storage and an iterator of SourceRecord objects from that.  Concrete implementation need only implement three (3) methods:

 * `SourceCommonConfig configure(Map<String, String> props)`
 * `Iterator<SourceRecord> getIterator(BackoffConfig config)`
 * `void closeResources()`

In addition, implementations will want to implement an `OffsetManager.OffsetManagerEntry`.  This class maps the information about the stored object to information describing how far the processing has progressed.  In general, the values that are needed to locate the original object in the object store are placed in the 'key' and data describing the processing is placed in the data.  There are no restrictions on either key or data elements except that they must be natively serializable by the Kafka process. See the [Kafka documentation](https://github.com/a0x8o/kafka/blob/master/connect/runtime/src/main/java/org/apache/kafka/connect/storage/OffsetStorageReaderImpl.java) for more information.



### Helper classes

The `Transformer` classes found in the source/input directory describe how to convert input streams into objects that the Kafka framework can handle.  The object read from the storage is converted into a stream of bytes and passed to a transformer.  The transformer then creates a stream of SchemaAndValue objects that are the values for the object, these will be sent to Kafka as values in SourceRecords.  The transformer also creates the key for the source record.  

Each SchemaAndValue in the stream from the transformer is associated with an OffsetManagerEntry. The OffsetManagerEntry, the key, and the value are all that is needed to construct a SourceRecord to be sent to the SourceTask in the SourceRecord Iterator.

The `ObjectDistributionStrategy` provides convenient methods to assign objects to tasks and should be used early in the processing of objects from storage to determine if the current task should handle them.

## General process

The `configure` method is called first.  The map provided is the data from the configuration.  The AbstractSourceTask implementation should create an instance of SourceCommonConfig and return it.  While it is doing this it should verify that all the data necessary for the SourceTask to operate has been defined.

Following the `configure` call, the  `getIterator` method will be called to initialize the process.  The iterator `hasnext()` method will return  `true` if there are any records available to be sent to Kafka.  It may return `false` for multiple calls to `hasNext()` before returning `true`.  When the iterator returns `true` for the `hasNext()` call, the data is read and prepared for delivery to Kafka.  If the iterator returns `false` the system will backoff and not request again for a period of time not exceeding 5 seconds.  In this way we do not overload the system with spurious requests.  If the data storage system has a mechanism by which it reports a recoverable error the iterator may use the associated `BackOff` instance to execute one or more delays.  This delay will increase in time until the 5-second limit is reached or the underlying timer expires.  The iterator should handle the case where all the data in the data storage has been read and then a new storage entry created.  It should do this by reporting `false` for all requests after the last entry has been read and before the new entry is created and then returning `true`.

`closeResources` is called when the Task is shutting down.  The underlying system should close all open connections and clean up within 5 seconds.  Any data that was read from the iterator will be sent.

## OffsetManager interactions

The OffsetManager will provide information about source data that the system has processed.  It does this by communicating with the Kafka offset topic.  The `OffsetManager.OffsetManagerEntry` provides the interface for the AbstractSourceTask to communicate with the OffsetManager entry.  The `OffsetManager` is a read only object, all information is retrieved from Kafka.  It has four (4) methods:

 * `Optional<E> getEntry(final OffsetManagerKey key, final Function<Map<String, Object>, E> creator)`  - This method retrieves any data Kafka has about the object identified by the key.  If the data are found then the `creator` function is called to create the OffsetManagerEntry implementation.  Otherwise, an empty Optional is returned.
 * `void remove(final OffsetManagerKey key)` - removes the data from the local OffsetManager cache.  This is called when data has been set to Kafka for the record identified by the key. Subsequent calls to `getEntry` will reload the data from Kafka.
 * `void remove(final SourceRecord sourceRecord)` - removes the data from the local OffsetManager cache.  This is called when the SourceRecord is sent to Kafka.  Subsequent calls to `getEntry` will reload the data from Kafka.
 * `void populateOffsetManager(Collection<OffsetManagerKey> keys)` - loads the data associated with a collection of keys into the OffsetManager cache so that they can be processed without further calls to retrieve the offset data from Kafka.

### OffsetManagerEntry

The `OffsetManagerEntry` is the base class for source specific `OffsetManager` data.  It contains and manages the key and data for the OffsetManager.  Every data source has a way to identify the data source within the system.  This information should be mapped into the OffsetManagerEntry key.  Every processing stream that converts data from the source representation to the final SourceRecord(s) has state that it needs to track.  The OffsetManagerEntry handles that information and makes it easy to store and retrieve it.  The key and the data are key/value maps where the key is a string and the value is an object.  The OffsetManagerEntry provides access to the map data via getter and setter methods and provides conversions to common data types. Implementations of OffsetManagerEntry should call the getters/setters to get/store data that will be written to the kafka storage.  The class may track any other data this is desired by the developers but only the data in the key and data maps will be preserved.

When thinking about the OffsetManagerEntry they key should be a simple map of key/value pairs that describe the location of the object in the storage system.  For something like S3 this would be the bucket and object key, for a database it would be table and primaryKey, other systems have other coordinate systems.  The `getManagerKey` method should return the map.  It is important to note that all integral numbers are stored in the Kafka maps as Longs.  So all integral key values should be stored a Longs in the map.  When integer data values are retrieved the `getInteger(String)` method may be used to ensure that the Long is converted into an Integer.

The data that is preserved in the data map should be used to store the state of the extraction of records from the source system.  This can include the number of records extracted from systems that store multiple records in an object, or a flag that says the object was processed, or any other information that will help the system determine if the object needs to continue processing.  The method `getProperties` should return this map.








