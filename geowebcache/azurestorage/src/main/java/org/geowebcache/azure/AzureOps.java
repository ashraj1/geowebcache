package org.geowebcache.azure;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import javax.annotation.Nullable;

import java.io.InputStream;
import java.net.URI;

import com.sun.jndi.toolkit.url.Uri;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.locks.LockProvider.Lock;
import org.geowebcache.locks.NoOpLockProvider;
import org.geowebcache.storage.*;

import org.geowebcache.storage.StorageException;
import org.h2.store.Storage;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Blob;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by jocollin on 27/02/2017.
 */
public class AzureOps {

    private final CloudBlobClient conn;

    private final CloudBlobContainer container;

    private final TMSKeyBuilder keyBuilder;

    private final LockProvider locks;

    private ExecutorService deleteExecutorService;

    private Map<String, Long> pendingDeletesKeyTime = new ConcurrentHashMap<>();

    public AzureOps(CloudBlobClient conn, String containerName, TMSKeyBuilder keyBuilder,
                 LockProvider locks) throws StorageException {
        this.conn = conn;
        this.keyBuilder = keyBuilder;
        this.locks = locks == null ? new NoOpLockProvider() : locks;

        try {
            this.container = this.conn.getContainerReference(containerName);
            this.container.createIfNotExists();
        } catch (Exception ex) {
            throw new StorageException("Failed establishing connection to container " + containerName);
        }
    }

    private ExecutorService createDeleteExecutorService() {
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("GWC AzureBlobStore bulk delete thread-%d. Bucket: " + container.getName())
                .setPriority(Thread.MIN_PRIORITY).build();
        return Executors.newCachedThreadPool(tf);
    }

    public void shutDown() {
        deleteExecutorService.shutdownNow();
    }

    private void issuePendingBulkDeletes() throws StorageException {
        final String pendingDeletesKey = keyBuilder.pendingDeletes();
        Lock lock;
        try {
            lock = locks.getLock(pendingDeletesKey);
        } catch (GeoWebCacheException e) {
            throw new StorageException("Unable to lock pending deletes", e);
        }

        try {
            Properties deletes = getProperties(pendingDeletesKey);
            for (Map.Entry<Object, Object> e : deletes.entrySet()) {
                final String prefix = e.getKey().toString();
                final long timestamp = Long.parseLong(e.getValue().toString());
                AzureBlobStore.log.info(String.format("Restarting pending bulk delete on '%s/%s':%d",
                        container.getName(), prefix, timestamp));
                asyncDelete(prefix, timestamp);
            }
        } finally {
            try {
                lock.release();
            } catch (GeoWebCacheException e) {
                throw new StorageException("Unable to unlock pending deletes", e);
            }
        }
    }

    private void clearPendingBulkDelete(final String prefix, final long timestamp)
            throws GeoWebCacheException {
        Long taskTime = pendingDeletesKeyTime.get(prefix);
        if (taskTime == null) {
            return; // someone else cleared it up for us. A task that run after this one but
            // finished before?
        }
        if (taskTime.longValue() > timestamp) {
            return;// someone else issued a bulk delete after this one for the same key prefix
        }
        final String pendingDeletesKey = keyBuilder.pendingDeletes();
        final Lock lock = locks.getLock(pendingDeletesKey);

        try {
            Properties deletes = getProperties(pendingDeletesKey);
            String storedVal = (String) deletes.remove(prefix);
            long storedTimestamp = storedVal == null ? Long.MIN_VALUE : Long.parseLong(storedVal);
            if (timestamp >= storedTimestamp) {
                putProperties(pendingDeletesKey, deletes);
            } else {
                AzureBlobStore.log.info(String.format(
                        "bulk delete finished but there's a newer one ongoing for bucket '%s/%s'",
                        this.container.getName(), prefix));
            }
        } catch (StorageException e) {
            Throwables.propagate(e);
        } finally {
            lock.release();
        }
    }

    public boolean scheduleAsyncDelete(final String prefix) throws GeoWebCacheException {
        final long timestamp = currentTimeSeconds();
        String msg = String.format("Issuing bulk delete on '%s/%s' for objects older than %d",
                container.getName(), prefix, timestamp);
        AzureBlobStore.log.info(msg);

        Lock lock = locks.getLock(prefix);
        try {
            boolean taskRuns = asyncDelete(prefix, timestamp);
            if (taskRuns) {
                final String pendingDeletesKey = keyBuilder.pendingDeletes();
                Properties deletes = getProperties(pendingDeletesKey);
                deletes.setProperty(prefix, String.valueOf(timestamp));
                putProperties(pendingDeletesKey, deletes);
            }
            return taskRuns;
        } catch (StorageException e) {
            throw Throwables.propagate(e);
        } finally {
            lock.release();
        }
    }

    private long currentTimeSeconds() {
        final long timestamp = (long) Math.ceil(System.currentTimeMillis() / 1000D) * 1000L;
        return timestamp;
    }

    public void putObject(ByteArrayInputStream input, long inputSize, String key) throws StorageException {
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.upload(input, inputSize);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    public byte[] getObject(String key) throws StorageException {
        byte[] byteArray = null;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            int fileByteLength = (int)blob.getProperties().getLength();
            byteArray = new byte[fileByteLength];
            blob.downloadToByteArray(byteArray, 0);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return byteArray;
    }

    private synchronized boolean asyncDelete(final String prefix, final long timestamp) {
        if (!prefixExists(prefix)) {
            return false;
        }

        Long currentTaskTime = pendingDeletesKeyTime.get(prefix);
        if (currentTaskTime != null && currentTaskTime.longValue() > timestamp) {
            return false;
        }

        BulkDelete task = new BulkDelete(conn, container.getName(), prefix, timestamp);
        deleteExecutorService.submit(task);
        pendingDeletesKeyTime.put(prefix, timestamp);

        return true;
    }

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

    public boolean prefixExists(String prefix) {
        boolean hasNext = container.listBlobs(prefix)
                .iterator().hasNext();
        return hasNext;
    }

    public HashMap<String, String> getMetadata(String key) throws StorageException {
        HashMap<String, String> metadata = new HashMap<String, String>();
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            metadata = blob.getMetadata();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return metadata;
    }

    public void setMetadata(String key, HashMap<String, String> metadata) throws StorageException {
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.setMetadata(metadata);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    public Properties getProperties(String key) throws StorageException {
        BlobProperties bprops;
        Properties props = new Properties();
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            bprops = blob.getProperties();
            props.put("BlobType", bprops.getBlobType());
            props.put("CopyState", bprops.getCopyState());
            props.put("AppendBlobCommitedBlockCount", bprops.getAppendBlobCommittedBlockCount());
            props.put("CacheControl", bprops.getCacheControl());
            props.put("ContentDisposition", bprops.getContentDisposition());
            props.put("ContentEncoding", bprops.getContentEncoding());
            props.put("ContentLanguage", bprops.getContentLanguage());
            props.put("ContentMD5", bprops.getContentMD5());
            props.put("ContentType", bprops.getContentType());
            props.put("Etag", bprops.getEtag());
            props.put("LastModified", bprops.getLastModified());
            props.put("LeaseDuration", bprops.getLeaseDuration());
            props.put("PageBlobSequenceNumber", bprops.getPageBlobSequenceNumber());
            props.put("IncrementalCopy", bprops.isIncrementalCopy());
            props.put("isServerEncrypted", bprops.isServerEncrypted());
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return props;
    }

    public BlobProperties getBlobProperties(String key) throws StorageException {
        BlobProperties bprops;
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            bprops = blob.getProperties();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
        return bprops;
    }

    public void setProperties(String key, BlobProperties props) throws StorageException {
        try {
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.uploadProperties();
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    public void putProperties(String resourceKey, Properties properties) throws StorageException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            properties.store(out, "");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        byte[] bytes = out.toByteArray();
        BlobProperties bprops = new BlobProperties();
        bprops.setContentType("text/plain");

        try {
            CloudBlockBlob blob = container.getBlockBlobReference(resourceKey);
            blob.uploadProperties();
        } catch(Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    private class BulkDelete implements Callable<Long> {

        private final String prefix;

        private final long timestamp;

        private final CloudBlobClient conn;

        private final String containerName;

        public BulkDelete(final CloudBlobClient conn, final String containerName, final String prefix,
                          final long timestamp) {
            this.conn = conn;
            this.containerName = containerName;
            this.prefix = prefix;
            this.timestamp = timestamp;
        }

        @Override
        public Long call() throws Exception {
            long count = 0L;
            try {
                checkInterrupted();
                AzureBlobStore.log.info(String.format("Running bulk delete on '%s/%s':%d", containerName,
                        prefix, timestamp));
                CloudBlobContainer container = conn.getContainerReference(containerName);
                Iterable<ListBlobItem> items = container.listBlobs(prefix);
                List<ListBlobItem> filtered = new ArrayList<ListBlobItem>();
                for (ListBlobItem item : items) {
                    CloudBlockBlob blob = container.getBlockBlobReference(item.getStorageUri().toString());
                    long lastModified = blob.getProperties().getLastModified().getTime();
                    if(lastModified < timestamp) {
                        filtered.add(item);
                    }
                }
                Iterable<List<ListBlobItem>> partitions = Iterables.partition(filtered, 1000);

                for (List<ListBlobItem> partition : partitions) {

                    checkInterrupted();

                    List<String> keys = new ArrayList<>(partition.size());
                    for (ListBlobItem so : partition) {
                        String key = so.getStorageUri().toString();
                        keys.add(key);
                    }

                    checkInterrupted();

                    if (!keys.isEmpty()) {

                        for(String key : keys) {
                            container.getBlockBlobReference(key);
                            container.deleteIfExists();
                        }
                        checkInterrupted();

                        count += keys.size();
                    }
                }
            } catch (InterruptedException | IllegalStateException e) {
                AzureBlobStore.log.info(String.format(
                        "S3 bulk delete aborted for '%s/%s'. Will resume on next startup.",
                        containerName, prefix));
                return null;
            } catch (Exception e) {
                AzureBlobStore.log.warn(String.format(
                        "Unknown error performing bulk S3 delete of '%s/%s'", containerName, prefix),
                        e);
                throw e;
            }

            AzureBlobStore.log.info(String.format(
                    "Finished bulk delete on '%s/%s':%d. %d objects deleted", containerName, prefix,
                    timestamp, count));

            AzureOps.this.clearPendingBulkDelete(prefix, timestamp);
            return count;
        }

        private void checkInterrupted() throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }
}
