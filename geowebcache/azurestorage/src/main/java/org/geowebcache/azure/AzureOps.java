package org.geowebcache.azure;

import com.microsoft.azure.storage.blob.*;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.storage.StorageException;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AzureOps {

    private final CloudBlobClient conn;

    private final CloudBlobContainer container;

    private final TMSKeyBuilder keyBuilder;

    private Map<String, Long> pendingDeletesKeyTime = new ConcurrentHashMap<>();

    public AzureOps(CloudBlobClient conn, String containerName, TMSKeyBuilder keyBuilder) throws StorageException {
        this.conn = conn;
        this.keyBuilder = keyBuilder;
        try {
            this.container = this.conn.getContainerReference(containerName);
            this.container.createIfNotExists();
        } catch (Exception ex) {
            throw new StorageException("Failed establishing connection to container " + containerName);
        }
    }

    public void shutdown() {
        // Clean up
    }

    /**
     * Put an object in Azure Blob Storage
     * @param input
     * @param inputSize
     * @param key
     * @throws StorageException
     */
    public void putObject(ByteArrayInputStream input, long inputSize, String key) throws StorageException {
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.upload(input, inputSize);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    /**
     * Get an existing object from Azure Blob Storage
     * @param key
     * @return
     * @throws StorageException
     */
    public byte[] getObject(String key) throws StorageException {
        byte[] byteArray = null;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            int fileByteLength = (int)getBlobProperties(key).getLength();
            byteArray = new byte[fileByteLength];
            blob.downloadToByteArray(byteArray, 0);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return byteArray;
    }

    /**
     * Synchronously delete all blobs with matching prefix in uri in Azure Blob Storage
     * @param prefix
     * @return
     * @throws StorageException
     */
    public boolean deleteBlobsWithMatchingPrefix(String prefix) throws StorageException {
        int deletedCount = 0;
        try {
            Iterable<ListBlobItem> blobs = container.listBlobs(prefix, true);
            for (ListBlobItem blob : blobs) {
                deletedCount += container.getBlockBlobReference(((CloudBlockBlob)blob).getName()).deleteIfExists() ? 1 : 0;
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return deletedCount > 0;
    }

    /**
     * Synchronously delete an existing object from Azure Blob Storage
     * @param key
     * @return
     * @throws StorageException
     */
    public boolean deleteObject(final String key) throws StorageException {
        boolean deleted = false;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            deleted = blob.deleteIfExists();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return deleted;
    }

    /**
     * Synchronously delete a list of objects from Azure Blob Storage
     * @param keys
     * @return
     * @throws StorageException
     */
    public boolean deleteObject(final List<String> keys) throws StorageException {
        boolean deleted = false;
        try {
            for(String key : keys) {
                CloudBlockBlob blob = container.getBlockBlobReference(key);
                deleted = blob.deleteIfExists();
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return deleted;
    }

    /**
     * Check whether a prefix exists on Azure Blob Storage
     * @param prefix
     * @return
     */
    public boolean prefixExists(String prefix) {
        boolean hasNext = container.listBlobs(prefix)
                .iterator().hasNext();
        return hasNext;
    }

    /**
     * Get a blob's metadata from Azure Blob Storage
     * @param key
     * @return
     * @throws StorageException
     */
    public HashMap<String, String> getBlobMetadata(String key) throws StorageException {
        HashMap<String, String> metadata = new HashMap<String, String>();
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            if(blob.exists()) {
                blob.downloadAttributes();
                metadata = blob.getMetadata();
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return metadata;
    }

    /**
     * Put a blob's metadata on Azure Blob Storage
     * @param key
     * @param metadata
     * @throws StorageException
     */
    public void putBlobMetadata(String key, HashMap<String, String> metadata) throws StorageException {
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            if(!blob.exists()) {
                blob.uploadText("");
            }
            blob.downloadAttributes();
            blob.setMetadata(metadata);
            blob.uploadMetadata();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    /**
     * Get a blob's properties from Azure Blob Storage
     * @param key
     * @return
     * @throws StorageException
     */
    public BlobProperties getBlobProperties(String key) throws StorageException {
        BlobProperties blobProps = null;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.downloadAttributes();
            blobProps = blob.getProperties();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return blobProps;
    }

    /**
     * Check whether a blob exists on Azure Blob Storage
     * @param key
     * @return
     * @throws StorageException
     */
    public boolean blobExists(String key) throws StorageException {
        boolean exists = false;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            exists = blob.exists();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return exists;
    }
}
