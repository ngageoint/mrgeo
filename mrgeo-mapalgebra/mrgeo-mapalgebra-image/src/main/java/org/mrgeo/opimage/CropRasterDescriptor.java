/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.opimage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.RenderedOp;
import javax.media.jai.registry.RenderedRegistryMode;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;

/**
 * Trades accuracy for speed.
 * 
 * @author jason.surratt
 * 
 */
public class CropRasterDescriptor extends OperationDescriptorImpl implements RenderedImageFactory
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(CropRasterDescriptor.class);

  public static final String OPERATION_NAME = CropRasterDescriptor.class.getName();
  private static final long serialVersionUID = 1L;
  public final static int CROP_TYPE = 4;

  public static RenderedOp create(RenderedImage source, double minX,
      double minY, double width, double height,
      int zoomLevel, int tileSize, String cropType)
  {
    return create(source, minX, minY, width, height,
        zoomLevel, tileSize, cropType, null);
  }

  public static RenderedOp create(RenderedImage source, double minX,
      double minY, double width, double height,
      int zoomLevel, int tileSize, String cropType, RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock())
        .addSource(source)
        .add(minX)
        .add(minY)
        .add(width)
        .add(height)
        .add(zoomLevel)
        .add(tileSize)
        .add(new String(cropType));
    return JAI.create(OPERATION_NAME, paramBlock, hints);
  }
  
  public CropRasterDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" },
        { "Description", "Crop a raster input in geographic coordinates" },
        { "DocURL", "http://www.spadac.com/" }, { "Version", "1.0" }, { "arg0Desc", "" },
        { "arg1Desc", "" }, { "arg2Desc", "" }, { "arg3Desc", "" }, { "arg4Desc", "" } },
        new String[] { RenderedRegistryMode.MODE_NAME },
        1,
        new String[] { "minX", "minY", "width", "height", "zoomLevel", "tileSize", "cropType" },
        new Class[] { Double.class, Double.class, Double.class, Double.class, Integer.class, Integer.class, String.class },
        new Object[] { NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT, NO_PARAMETER_DEFAULT },
        null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * java.awt.image.renderable.RenderedImageFactory#create(java.awt.image.renderable
   * .ParameterBlock, java.awt.RenderingHints)
   */
  @Override
  public RenderedImage create(ParameterBlock paramBlock, RenderingHints hints)
  {
    double minX = paramBlock.getDoubleParameter(0);
    double minY = paramBlock.getDoubleParameter(1);
    double width = paramBlock.getDoubleParameter(2);
    double height = paramBlock.getDoubleParameter(3);
    int zoomLevel = paramBlock.getIntParameter(4);
    int tileSize = paramBlock.getIntParameter(5);
    String cropType = (String)paramBlock.getObjectParameter(6);

    return CropRasterOpImage.create(paramBlock.getRenderedSource(0),
        minX, minY, width, height, zoomLevel, tileSize, cropType);
  }
}
