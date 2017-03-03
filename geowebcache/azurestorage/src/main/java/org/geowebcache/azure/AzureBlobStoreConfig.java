package org.geowebcache.azure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.geowebcache.config.BlobStoreConfig;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.StorageException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

/**
 * Created by jocollin on 27/02/2017.
 */
public class AzureBlobStoreConfig extends BlobStoreConfig {

    static Log log = LogFactory.getLog(AzureBlobStoreConfig.class);

    private String container;

    private String azureAccountName;

    private String azureAccountKey;

    public String getContainer() {
        return this.container;
    }

    public void setContainer(String container) {
        this.container = container;
    }

    public String getAzureAccountName() {
        return this.azureAccountName;
    }

    public void setAzureAccountName(String accountName) {
        this.azureAccountName = accountName;
    }

    public String getAzureAccountKey() {
        return this.azureAccountKey;
    }

    public void setAzureAccountKey(String accountKey) {
        this.azureAccountKey = accountKey;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public String getLocation() {
        String container = this.getContainer();
        String prefix = this.getPrefix();
        if(prefix==null){
            return String.format("container: %s", container);
        } else {
            return String.format("container: %s ", container, prefix);
        }
    }

    @Override
    public BlobStore createInstance(TileLayerDispatcher layers, LockProvider lockProvider)
            throws StorageException {

        checkNotNull(layers);
        checkState(getId() != null);
        checkState(isEnabled(),
                "Can't call AzureBlobStoreConfig.createInstance() is blob store is not enabled");
        return new AzureBlobStore(this, layers);
    }

    public CloudBlobClient buildClient() {
        String storageConnectionString =
                "DefaultEndpointsProtocol=http;"
                        + "AccountName=" + azureAccountName + ";"
                        + "AccountKey=" + azureAccountKey;
        CloudBlobClient serviceClient = null;
        try {
            CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
            log.debug("Initializing Azure Blob Storage connection");
            serviceClient = account.createCloudBlobClient();
            return serviceClient;
        } catch(Exception ex) {

        }
        return serviceClient;
    }
}
