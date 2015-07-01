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

package org.mrgeo.opimage;

import javax.media.jai.JAI;
import javax.media.jai.OperationDescriptorImpl;
import javax.media.jai.RenderedOp;
import javax.media.jai.operator.LogDescriptor;
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
public class LogarithmRasterDescriptor extends OperationDescriptorImpl implements RenderedImageFactory
{
  public static final String OPERATION_NAME = LogarithmRasterDescriptor.class.getName();
  private static final long serialVersionUID = 1L;
  public final static int BASE = 0;

  public static RenderedOp create(RenderedImage source, double base)
  {
    return create(source, base, null);
  }

  public static RenderedOp create(RenderedImage source, double base, RenderingHints hints)
  {
    ParameterBlock paramBlock = (new ParameterBlock()).addSource(source).add(new Double(base));
    return JAI.create(OPERATION_NAME, paramBlock, hints);
  }

  public LogarithmRasterDescriptor()
  {
    // I realize this formatting is horrendous, but Java won't let me assign
    // variables before
    // calling super.
    super(new String[][] { { "GlobalName", OPERATION_NAME }, { "LocalName", OPERATION_NAME },
        { "Vendor", "com.spadac" },
        { "Description", "Crop a raster input in geographic coordinates" },
        { "DocURL", "http://www.spadac.com/" }, { "Version", "1.0" }, { "arg0Desc", "" },
        { "arg1Desc", "" }, { "arg2Desc", "" }, { "arg3Desc", "" }, { "arg4Desc", "" } },
        new String[] { RenderedRegistryMode.MODE_NAME }, 0, new String[] { "base" },
        new Class[] { Double.class },
        new Object[] { NO_PARAMETER_DEFAULT }, null);
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
    //Vector<RenderedImage> sources = new Vector<RenderedImage>();
    //sources.add(paramBlock.getRenderedSource(0));
    
    RenderedImage input = paramBlock.getRenderedSource(0);
    
    int base = (int) paramBlock.getDoubleParameter(BASE);
    if (base > 0 && base != 1)
    {
      return LogWithBaseOpImage.create(input, base, hints);
    }
    
    //return the natural logarithm of the pixel values if base is not defined
    return LogDescriptor.create(input, hints); 
  }
}
