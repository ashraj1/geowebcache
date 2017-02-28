package org.geowebcache.azure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by jocollin on 27/02/2017.
 */
public class PropertiesLoader {

    private static Log log = LogFactory.getLog(PropertiesLoader.class);

    private Properties properties = new Properties();

    public PropertiesLoader() {
        String home = System.getProperty("user.home");
        File configFile = new File(home, ".gwc_azure_tests.properties");
        log.info("Loading Azure tests config. File must have keys 'containerName', 'accountName', and 'accountKey'");
        if (configFile.exists()) {
            try (InputStream in = new FileInputStream(configFile)) {
                properties.load(in);
                checkArgument(null != properties.getProperty("containerName"),
                        "container not provided in config file " + configFile.getAbsolutePath());
                checkArgument(null != properties.getProperty("accountName"),
                        "accountName not provided in config file " + configFile.getAbsolutePath());
                checkArgument(null != properties.getProperty("accountKey"),
                        "accountKey not provided in config file " + configFile.getAbsolutePath());
            } catch (IOException e) {
                log.fatal("Error loading Azure tests config: " + configFile.getAbsolutePath(), e);
            }
        } else {
            log.warn("Azure Blob storage config file not found. GWC Azure Blob tests will be ignored. "
                    + configFile.getAbsolutePath());
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
