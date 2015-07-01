/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.utils.Bounds;

import java.awt.image.Raster;
import java.util.Properties;


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
   * @return
   */
  double[] getDefaultValues();

  /**
   * Returns the minimum/maximum raster values from the source data
   * 
   * @return
   */
  double[] getExtrema();

  /**
   * Returns the mime types for the image renderer
   * 
   * @return a mime type string
   */
  String[] getMimeTypes();

  /**
   * Returns the WMS format types for the image renderer
   * 
   * @return a format string
   */
  String[] getWmsFormats();

  /**
   * Determines if the rendered image was rendered as transparent
   * 
   * @return true if the resulting image output is transparent; false otherwise
   * @todo semi-hack
   */
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
   * @throws Exception
   */
  Raster renderImage(String pyramidName, Bounds bounds, int width, int height, Properties providerProperties,
    String epsg) throws Exception;

  Raster renderImage(String pyramidName, Bounds bounds, Properties providerProperties, String epsg)
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
   * @throws Exception
   */
  Raster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
      Properties providerProperties) throws Exception;

  Raster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
    String maskName, double maskMax,
    Properties providerProperties) throws Exception;

  Raster renderImage(String pyramidName, int tileColumn, int tileRow, double scale,
    String maskName, Properties providerProperties) throws Exception;

  Raster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom,
      Properties providerProperties) throws Exception;

  Raster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom, String maskName,
    double maskMax, Properties providerProperties) throws Exception;

  Raster renderImage(String pyramidName, int tileColumn, int tileRow, int zoom, String maskName,
      final Properties providerProperties) throws Exception;
}
