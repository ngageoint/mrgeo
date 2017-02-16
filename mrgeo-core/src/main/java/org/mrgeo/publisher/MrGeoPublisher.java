package org.mrgeo.publisher;

import org.mrgeo.image.MrsPyramidMetadata;

/**
 * Created by ericwood on 8/9/16.
 */
public interface MrGeoPublisher
{

/**
 * @param imageMetadata
 */
void publishImage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException;

}
