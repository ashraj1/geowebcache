package org.geowebcache.azure;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.BlobStoreListenerList;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;


/**
 * Created by jocollin on 27/02/2017.
 */
public class AzureBlobStore implements BlobStore {

    static Log log = LogFactory.getLog(AzureBlobStore.class);

    private final BlobStoreListenerList listeners = new BlobStoreListenerList();

    private CloudBlobClient conn;

    private final TMSKeyBuilder keyBuilder;

    private String containerName;

    private volatile boolean shutDown;

    private final AzureOps azureOps;

    public AzureBlobStore(AzureBlobStoreConfig config, TileLayerDispatcher layers,
                          LockProvider lockProvider) throws StorageException {
        checkNotNull(config);
        checkNotNull(layers);
        checkNotNull(config.getAzureAccountName(), "Account name not provided");
        checkNotNull(config.getAzureAccountKey(), "Account key not provided");

        this.containerName = config.getContainer();
        String prefix = config.getPrefix() == null ? "" : config.getPrefix();
        this.keyBuilder = new TMSKeyBuilder(prefix, layers);

        conn = config.buildClient();

        this.azureOps = new AzureOps(conn, containerName, keyBuilder, lockProvider);
    }

    @Override
    public void destroy() {
        this.shutDown = true;
        CloudBlobClient conn = this.conn;
        this.conn = null;
        if(conn != null) {
            azureOps.shutDown();
        }
    }

    @Override
    public void addListener(BlobStoreListener listener) {
        listeners.addListener(listener);
    }

    @Override
    public boolean removeListener(BlobStoreListener listener) {
        return listeners.removeListener(listener);
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        final Resource blob = obj.getBlob();
        checkNotNull(blob);
        checkNotNull(obj.getBlobFormat());

        final String key = keyBuilder.forTile(obj);

        String blobFormat = obj.getBlobFormat();
        String mimeType;
        try {
            mimeType = MimeType.createFromFormat(blobFormat).getMimeType();
        } catch (MimeException me) {
            throw Throwables.propagate(me);
        };
        azureOps.getBlobProperties(key).setContentType(mimeType);

        final ByteArrayInputStream input = toByteArray(blob);
        log.trace(log.isTraceEnabled() ? ("Storing " + key) : "");
        azureOps.putObject(input, blob.getSize(), key);
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        final String key = keyBuilder.forTile(obj);
        try {
             byte[] bytes = azureOps.getObject(key);
             if(bytes == null) {
                return false;
             }
                obj.setBlobSize(bytes.length);
                obj.setBlob(new ByteArrayResource(bytes));
                obj.setCreated(azureOps.getBlobProperties(key).getLastModified().getTime());
        } catch (IOException e) {
            throw new StorageException("Error getting " + key, e);
        }

        return true;
    }

    private ByteArrayInputStream toByteArray(final Resource blob) throws StorageException {
        final byte[] bytes;
        if (blob instanceof ByteArrayResource) {
            bytes = ((ByteArrayResource) blob).getContents();
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream((int) blob.getSize());
            WritableByteChannel channel = Channels.newChannel(out);
            try {
                blob.transferTo(channel);
            } catch (IOException e) {
                throw new StorageException("Error copying blob contents", e);
            }
            bytes = out.toByteArray();
        }
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        return input;
    }

    @Override
    public boolean delete(final TileRange tileRange) throws StorageException {
        final String coordsPrefix = keyBuilder.coordinatesPrefix(tileRange);
        if(!azureOps.prefixExists(coordsPrefix)) {
            return false;
        }

        final Iterator<long[]> tileLocations = new AbstractIterator<long[]>() {

            // TileRange iterator with 1x1 meta tiling factor
            private TileRangeIterator trIter = new TileRangeIterator(tileRange, new int[] { 1, 1 });

            @Override
            protected long[] computeNext() {
                long[] gridLoc = trIter.nextMetaGridLocation(new long[3]);
                return gridLoc == null ? endOfData() : gridLoc;
            }
        };

        if(listeners.isEmpty()) {
            // if there are no listeners, don't bother requesting every tile
            // metadata to notify the listeners
            Iterator<List<long[]>> partition = Iterators.partition(tileLocations, 1000);
            final TileToKey tileToKey = new TileToKey(coordsPrefix, tileRange.getMimeType());

            while (partition.hasNext() && !shutDown) {
                List<long[]> locations = partition.next();
                List<String> keys = Lists.transform(locations, tileToKey);
                azureOps.deleteObject(keys);
            }
        } else {
            long[] xyz;
            String layerName = tileRange.getLayerName();
            String gridSetId = tileRange.getGridSetId();
            String format = tileRange.getMimeType().getFormat();
            Map<String, String> parameters = tileRange.getParameters();

            while (tileLocations.hasNext()) {
                xyz = tileLocations.next();
                TileObject tile = TileObject.createQueryTileObject(layerName, xyz, gridSetId,
                        format, parameters);
                tile.setParametersId(tileRange.getParametersId());
                delete(tile);
            }
        }

        return true;
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        checkNotNull(layerName, "layerName");

        final String metadataKey = keyBuilder.layerMetadata(layerName);
        final String layerPrefix = keyBuilder.forLayer(layerName);

        azureOps.deleteObject(metadataKey);

        boolean layerExists;
        try {
            layerExists = azureOps.scheduleAsyncDelete(layerPrefix);
        } catch (GeoWebCacheException e) {
            throw Throwables.propagate(e);
        }
        if (layerExists) {
            listeners.sendLayerDeleted(layerName);
        }
        return layerExists;
    }

    @Override
    public boolean deleteByGridsetId(final String layerName, final String gridSetId)
            throws StorageException {
        checkNotNull(layerName, "layerName");
        checkNotNull(gridSetId, "gridSetId");

        final String gridsetPrefix = keyBuilder.forGridset(layerName, gridSetId);

        boolean prefixExists;
        try {
            prefixExists = azureOps.scheduleAsyncDelete(gridsetPrefix);
        } catch (GeoWebCacheException e) {
            throw Throwables.propagate(e);
        }
        if (prefixExists) {
            listeners.sendGridSubsetDeleted(layerName, gridSetId);
        }
        return prefixExists;
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        final String key = keyBuilder.forTile(obj);

        // don't bother for the extra call if there are no listeners
        if (listeners.isEmpty()) {
            return azureOps.deleteObject(key);
        }

        HashMap<String, String> oldObj = azureOps.getMetadata(key);

        if (oldObj == null) {
            return false;
        }

        azureOps.deleteObject(key);
        obj.setBlobSize(Integer.parseInt(oldObj.get("Content-Length")));
        listeners.sendTileDeleted(obj);
        return true;
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        log.debug("No need to rename layers, AzureBlobStore uses layer id as key root");
        if (azureOps.prefixExists(oldLayerName)) {
            listeners.sendLayerRenamed(oldLayerName, newLayerName);
        }
        return true;
    }

    @Override
    public void clear() throws StorageException {
        throw new UnsupportedOperationException("clear() should not be called");
    }

    @Nullable
    @Override
    public String getLayerMetadata(String layerName, String key) {
        Properties properties = getLayerMetadata(layerName);
        String value = properties.getProperty(key);
        return value;
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        Properties properties = getLayerMetadata(layerName);
        properties.setProperty(key, value);
        String resourceKey = keyBuilder.layerMetadata(layerName);
        try {
            azureOps.putProperties(resourceKey, properties);
        } catch (StorageException e) {
            Throwables.propagate(e);
        }
    }

    private Properties getLayerMetadata(String layerName) {
        String key = keyBuilder.layerMetadata(layerName);
        Properties props = null;
        try {
            props = azureOps.getProperties(key);
        } catch (Exception ex) {
            log.trace(ex.getMessage());
        }
        return props;
    }

    @Override
    public boolean layerExists(String layerName) {
        final String coordsPrefix = keyBuilder.forLayer(layerName);
        boolean layerExists = azureOps.prefixExists(coordsPrefix);
        return layerExists;
    }

    private class TileToKey implements Function<long[], String> {

        private final String coordsPrefix;

        private final String extension;

        public TileToKey(String coordsPrefix, MimeType mimeType) {
            this.coordsPrefix = coordsPrefix;
            this.extension = mimeType.getInternalName();
        }

        @Override
        public String apply(long[] loc) {
            long z = loc[2];
            long x = loc[0];
            long y = loc[1];
            StringBuilder sb = new StringBuilder(coordsPrefix);
            sb.append(z).append('/').append(x).append('/').append(y).append('.').append(extension);
            return sb.toString();
        }

    }
}
