/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.utils.tms.Bounds;


/**
 * Interface to implement for classes who render images for WMS requests; Implementing class names
 * must begin with the image format name they handle (e.g. "Png") and end with "ImageRenderer". This
 * class must stay in the same namespace as ImageHandlerFactory.
 */
public interface ImageRenderer
{

/**
 * Returns the default value used in the source raster data
 *
 */
double[] getDefaultValues();

/**
 * Returns the minimum/maximum raster values from the source data
 *
 */
double[] getExtrema();

/**
 * Returns the mime types for the image renderer
 **/
String[] getMimeTypes();

/**
 * Returns the WMS format types for the image renderer
 *
 */
String[] getWmsFormats();

/**
 * Determines if the rendered image was rendered as transparent
 **/
boolean outputIsTransparent();

/**
 * GetMap implementation
 *
 * @param pyramidName
 *          name of the source data layer
 * @param bounds
 *          requested bounds
 * @param width
 *          requested image width
 * @param height
 *          requested image height
 * @return rendered image
 */
MrGeoRaster renderImage(String pyramidName, Bounds bounds, int width, int height, ProviderProperties providerProperties,
    String epsg) throws Exception;

MrGeoRaster renderImage(String pyramidName, Bounds bounds, ProviderProperties providerProperties, String epsg)
    throws Exception;

/**
 * GetTile implementation
 *
 * @param pyramidName
 *          name of the source data layer
 * @param tileColumn
 *          x tile coordinate
 * @param tileRow
 *          y tile coordinate
 * @param scale
 *          requested image resolution
 * @return rendered image
 */
MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
    ProviderProperties providerProperties) throws Exception;

MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
    String maskName, double maskMax,
    ProviderProperties providerProperties) throws Exception;

MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
    String maskName, ProviderProperties providerProperties) throws Exception;

MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom,
    ProviderProperties providerProperties) throws Exception;

MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom, String maskName,
    double maskMax, ProviderProperties providerProperties) throws Exception;

MrGeoRaster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom, String maskName,
    final ProviderProperties providerProperties) throws Exception;
}
