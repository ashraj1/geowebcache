package org.geowebcache.azure;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.h2.store.Storage;
import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by jocollin on 27/02/2017.
 */
public class TemporaryAzureBlobFolder extends ExternalResource {

    private Properties properties;

    private String containerName;

    private CloudBlobContainer container;

    private String accountName;

    private String accountKey;

    private String temporaryPrefix;

    private CloudBlobClient azure;

    public TemporaryAzureBlobFolder(Properties properties) {
        this.properties = properties;
        this.containerName = properties.getProperty("containerName");
        this.accountName = properties.getProperty("accountName");
        this.accountKey = properties.getProperty("accountKey");
    }

    @Override
    protected void before() throws Throwable {
        if (!isConfigured()) {
            return;
        }
        this.temporaryPrefix = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        azure = getConfig().buildClient();

        try {
            this.container = azure.getContainerReference(this.containerName);
        } catch (Exception ex) {
            // Yawn
        }
    }

    @Override
    protected void after() {
        if (!isConfigured()) {
            return;
        }
        try {
            delete();
        } finally {
            temporaryPrefix = null;
            //azure.shutdown;
        }
    }

    public CloudBlobClient getClient() {
        checkState(isConfigured(), "client not configured.");
        return azure;
    }

    public AzureBlobStoreConfig getConfig() {
        checkState(isConfigured(), "client not configured.");
        AzureBlobStoreConfig config = new AzureBlobStoreConfig();
        config.setContainer(containerName);
        config.setAzureAccountKey(accountKey);
        config.setAzureAccountName(accountName);
        config.setPrefix(temporaryPrefix);

        return config;
    }

    public void delete() {
        checkState(isConfigured(), "client not configured.");
        if (temporaryPrefix == null) {
            return;
        }

        Iterable<ListBlobItem> items = container.listBlobs(temporaryPrefix);
        Iterable<List<ListBlobItem>> partition = Iterables.partition(items, 1000);
        for (List<ListBlobItem> os : partition) {
            List<String> keys = Lists.transform(os,
                    new Function<ListBlobItem, String>() {
                        @Override
                        public String apply(ListBlobItem input) {
                            String k = input.getStorageUri().toString();
                            return k;
                        }
                    });
            for(String key : keys) {
                try {
                    CloudBlockBlob blob = container.getBlockBlobReference(key);
                    blob.deleteIfExists();
                } catch (Exception ex) {
                    // Yawn
                }
            }
        }
    }

    public boolean isConfigured() {
        return containerName != null && accountKey != null && accountName != null;
    }
}
