package org.mrgeo.publisher;

import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.mapalgebra.raster.RasterMapOp;

/**
 * Created by ericwood on 8/9/16.
 */
public interface MrGeoPublisher {

    /**
     *
     * @param imageMetadata
     */
    void publishImage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException;

}
