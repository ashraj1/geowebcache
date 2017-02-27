package org.geowebcache.azure;

import com.google.common.base.Throwables;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.blobstore.file.FilePathGenerator;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jocollin on 27/02/2017.
 */
final class TMSKeyBuilder {
    /**
     * Key format, comprised of
     * {@code <prefix>/<layer name>/<gridset id>/<format id>/<parameters hash>/<z>/<x>/<y>.<extension>}
     */
    private static final String TILE_FORMAT = "%s/%s/%s/%s/%s/%d/%d/%d.%s";

    /**
     * Coordinates prefix: {@code <prefix>/<layer name>/<gridset id>/<format id>/<parameters hash>/}
     */
    private static final String COORDINATES_PREFIX_FORMAT = "%s/%s/%s/%s/%s/";

    /**
     * Layer prefix format, comprised of {@code <prefix>/<layer name>/}
     */
    private static final String LAYER_PREFIX_FORMAT = "%s/%s/";

    /**
     * layer + gridset prefix format, comprised of {@code <prefix>/<layer name>/<gridset id>/}
     */
    private static final String GRIDSET_PREFIX_FORMAT = "%s/%s/%s/";

    public static final String LAYER_METADATA_OBJECT_NAME = "metadata.properties";

    private static final String LAYER_METADATA_FORMAT = "%s/%s/" + LAYER_METADATA_OBJECT_NAME;

    private String prefix;

    private TileLayerDispatcher layers;

    public TMSKeyBuilder(final String prefix, TileLayerDispatcher layers) {
        this.prefix = prefix;
        this.layers = layers;
    }

    public String layerId(String layerName) {
        TileLayer layer;
        try {
            layer = layers.getTileLayer(layerName);
        } catch (GeoWebCacheException e) {
            throw Throwables.propagate(e);
        }
        return layer.getId();
    }

    public String forTile(TileObject obj) {
        checkNotNull(obj.getLayerName());
        checkNotNull(obj.getGridSetId());
        checkNotNull(obj.getBlobFormat());
        checkNotNull(obj.getXYZ());

        String layer = layerId(obj.getLayerName());
        String gridset = obj.getGridSetId();
        String shortFormat;
        String parametersId = obj.getParametersId();
        if (parametersId == null) {
            Map<String, String> parameters = obj.getParameters();
            parametersId = FilePathGenerator.getParametersId(parameters);
            if (parametersId == null) {
                parametersId = "default";
            } else {
                obj.setParametersId(parametersId);
            }
        }
        Long x = Long.valueOf(obj.getXYZ()[0]);
        Long y = Long.valueOf(obj.getXYZ()[1]);
        Long z = Long.valueOf(obj.getXYZ()[2]);
        String extension;
        try {
            String format = obj.getBlobFormat();
            MimeType mimeType = MimeType.createFromFormat(format);
            shortFormat = mimeType.getFileExtension();// png, png8, png24, etc
            extension = mimeType.getInternalName();// png, jpeg, etc
        } catch (MimeException e) {
            throw Throwables.propagate(e);
        }

        String key = String.format(TILE_FORMAT, prefix, layer, gridset, shortFormat, parametersId,
                z, x, y, extension);
        return key;
    }

    public String forLayer(final String layerName) {
        String layerId = layerId(layerName);
        return String.format(LAYER_PREFIX_FORMAT, prefix, layerId);
    }

    public String forGridset(final String layerName, final String gridsetId) {
        String layerId = layerId(layerName);
        return String.format(GRIDSET_PREFIX_FORMAT, prefix, layerId, gridsetId);
    }

    public String layerMetadata(final String layerName) {
        String layerId = layerId(layerName);
        return String.format(LAYER_METADATA_FORMAT, prefix, layerId);
    }

    /**
     * @return the key prefix up to the coordinates (i.e.
     *         {@code "<prefix>/<layer>/<gridset>/<format>/<parametersId>"})
     */
    public String coordinatesPrefix(TileRange obj) {
        checkNotNull(obj.getLayerName());
        checkNotNull(obj.getGridSetId());
        checkNotNull(obj.getMimeType());

        String layer = layerId(obj.getLayerName());
        String gridset = obj.getGridSetId();
        MimeType mimeType = obj.getMimeType();

        String shortFormat;
        String parametersId = obj.getParametersId();
        if (parametersId == null) {
            Map<String, String> parameters = obj.getParameters();
            parametersId = FilePathGenerator.getParametersId(parameters);
            if (parametersId == null) {
                parametersId = "default";
            } else {
                obj.setParametersId(parametersId);
            }
        }
        shortFormat = mimeType.getFileExtension();// png, png8, png24, etc

        String key = String.format(COORDINATES_PREFIX_FORMAT, prefix, layer, gridset, shortFormat,
                parametersId);
        return key;
    }

    public String pendingDeletes() {
        return String.format("%s/%s", prefix, "_pending_deletes.properties");
    }
}
