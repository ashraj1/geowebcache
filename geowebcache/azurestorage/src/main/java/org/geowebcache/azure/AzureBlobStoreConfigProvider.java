package org.geowebcache.azure;

import com.google.common.base.Strings;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.SingleValueConverter;
import com.thoughtworks.xstream.converters.basic.BooleanConverter;
import com.thoughtworks.xstream.converters.basic.IntConverter;
import org.geowebcache.config.XMLConfigurationProvider;

public class AzureBlobStoreConfigProvider implements XMLConfigurationProvider {

    private static SingleValueConverter NullableIntConverter = new IntConverter() {

        @Override
        public Object fromString(String str) {
            if (Strings.isNullOrEmpty(str)) {
                return null;
            }
            return super.fromString(str);
        }
    };

    private static SingleValueConverter NullableBooleanConverter = new BooleanConverter() {

        @Override
        public Object fromString(String str) {
            if (Strings.isNullOrEmpty(str)) {
                return null;
            }
            return super.fromString(str);
        }
    };

    @Override
    public XStream getConfiguredXStream(XStream xs) {
        xs.alias("AzureBlobStore", AzureBlobStoreConfig.class);
        return xs;
    }
}
