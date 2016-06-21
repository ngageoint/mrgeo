package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.image.ImageInputFormatContext;
import org.mrgeo.utils.tms.Bounds;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/10/16.
 */
public class ConfigurationBuilder {
    // Key names from ImageInputFormatContext
    private static final String className = ImageInputFormatContext.class.getSimpleName();
    private static final String ZOOM_LEVEL = className + ".zoomLevel";
    private static final String TILE_SIZE = className + ".tileSize";
    private static final String INPUT = className + ".input";
    private static final String BOUNDS = className + ".bounds";
    private static final String PROVIDER_PROPERTY_KEY = className + "provProps";

    private final Configuration configuration;
    private int zoomLevel;
    private int tileSize;
    private String boundsString;

    public ConfigurationBuilder() {
        this.configuration = mock(Configuration.class);
    }

    public ConfigurationBuilder zoomLevel(int zoomLevel) {
        this.zoomLevel = zoomLevel;

        return this;
    }

    public ConfigurationBuilder tileSize(int tileSize) {
        this.tileSize = tileSize;

        return this;
    }

    public ConfigurationBuilder boundsString(String boundsString) {
        this.boundsString = boundsString;

        return this;
    }

    public Configuration build() {
        when(configuration.getInt(ZOOM_LEVEL, 1)).thenReturn(zoomLevel);
        when(configuration.getInt(TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT)).thenReturn(tileSize);
        when(configuration.get(BOUNDS)).thenReturn(boundsString);

        return configuration;
    }
}
